using Batch.Abstractions;
using Batch.Core.Execution;
using Batch.Core.Locking;
using Batch.Core.Persistence;
using Batch.Host.Cli;
using Microsoft.Extensions.DependencyInjection;

namespace Batch.IntegrationTests;

[Collection(PostgresCollection.Name)]
public sealed class PostgresEndToEndTests(PostgresFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Import_invoices_end_to_end_with_skips_and_idempotent_restart()
    {
        var file = await WriteCsvAsync(
            "invoice_number;customer_id;issue_date;amount;currency",
            "T1-0001;C1;2026-01-01;10.00;EUR",
            "T1-0002;C2;2026-01-02;20.00;XOF",
            "T1-0003;C3;not-a-date;30.00;EUR",
            "T1-0004;C4;2026-01-04;40.00;USD",
            "T1-0002;C2;2026-01-02;25.00;XOF"); // doublon : la dernière ligne gagne

        await using var sp = postgres.BuildServices(new() { ["Batch:Jobs:import-invoices:ChunkSize"] = "2" });
        var runner = sp.GetRequiredService<IJobRunner>();
        var parameters = new Dictionary<string, string?> { ["InputFile"] = file };

        var first = await runner.RunAsync(new RunRequest("import-invoices", parameters, JobTrigger.Test), Ct);

        Assert.Equal(ExitCodes.CompletedWithWarnings, first.ExitCode);
        Assert.Equal(new JobCounters(5, 4, 1), first.Counters);
        Assert.Equal(3, await postgres.ScalarAsync("SELECT count(*) FROM billing.invoice WHERE invoice_number LIKE 'T1-%'"));
        Assert.Equal(25, await postgres.ScalarAsync("SELECT amount::bigint FROM billing.invoice WHERE invoice_number = 'T1-0002'"));

        // Rejouer l'import complet ne crée aucun doublon : l'écriture est idempotente.
        var replay = await runner.RunAsync(new RunRequest("import-invoices", parameters, JobTrigger.Test), Ct);
        Assert.Equal(ExitCodes.CompletedWithWarnings, replay.ExitCode);
        Assert.Equal(3, await postgres.ScalarAsync("SELECT count(*) FROM billing.invoice WHERE invoice_number LIKE 'T1-%'"));

        var journal = await sp.GetRequiredService<IJobExecutionStore>().ListRecentAsync("import-invoices", 10, Ct);
        Assert.Contains(journal, r => r.RunId == first.RunId && r.Status == JobStatus.CompletedWithWarnings && r.Checkpoint == "6");
    }

    [Fact]
    public async Task Restart_imports_a_new_file_dropped_at_the_same_path()
    {
        // Le cas de la CronJob nocturne : toujours le même chemin, toujours --restart.
        var file = await WriteCsvAsync(
            "invoice_number;customer_id;issue_date;amount;currency",
            "T2-0001;C1;2026-01-01;10.00;EUR");

        await using var sp = postgres.BuildServices();
        var runner = sp.GetRequiredService<IJobRunner>();
        var request = new RunRequest("import-invoices", new Dictionary<string, string?> { ["InputFile"] = file }, JobTrigger.Test, Restart: true);

        var night1 = await runner.RunAsync(request, Ct);
        Assert.Equal(ExitCodes.Success, night1.ExitCode);

        // Même fichier toujours présent : rien à refaire.
        var sameFile = await runner.RunAsync(request, Ct);
        Assert.Equal(night1.RunId, sameFile.RunId);

        // Nouveau fichier déposé au même chemin : il est importé.
        await File.WriteAllLinesAsync(
            file,
            ["invoice_number;customer_id;issue_date;amount;currency", "T2-0002;C2;2026-01-02;20.00;EUR"],
            Ct);
        var night2 = await runner.RunAsync(request, Ct);

        Assert.Equal(ExitCodes.Success, night2.ExitCode);
        Assert.NotEqual(night1.RunId, night2.RunId);
        Assert.Equal(1, night2.Counters.Written);
        Assert.Equal(2, await postgres.ScalarAsync("SELECT count(*) FROM billing.invoice WHERE invoice_number LIKE 'T2-%'"));

        var journal = await sp.GetRequiredService<IJobExecutionStore>().ListRecentAsync("import-invoices", 50, Ct);
        Assert.StartsWith("sha256:", journal.Single(r => r.RunId == night2.RunId).InputIdentity, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_input_file_is_a_functional_error()
    {
        await using var sp = postgres.BuildServices();
        var outcome = await sp.GetRequiredService<IJobRunner>().RunAsync(
            new RunRequest("import-invoices", new Dictionary<string, string?> { ["InputFile"] = "/nope/missing.csv" }, JobTrigger.Test),
            Ct);

        Assert.Equal(ExitCodes.FunctionalError, outcome.ExitCode);
    }

    [Fact]
    public async Task Advisory_lock_is_exclusive_across_processes_and_released_on_dispose()
    {
        // Deux conteneurs DI distincts = deux « processus » qui partagent seulement la base.
        await using var nodeA = postgres.BuildServices();
        await using var nodeB = postgres.BuildServices();
        var locksA = nodeA.GetRequiredService<IJobLockProvider>();
        var locksB = nodeB.GetRequiredService<IJobLockProvider>();
        var key = $"batch:test:{Guid.NewGuid():N}";

        await using (var held = await locksA.TryAcquireAsync(key, Ct))
        {
            Assert.NotNull(held);
            Assert.Null(await locksB.TryAcquireAsync(key, Ct));
        }

        await using var reacquired = await locksB.TryAcquireAsync(key, Ct);
        Assert.NotNull(reacquired);
    }

    [Fact]
    public async Task Purge_removes_only_old_finished_executions()
    {
        await using var sp = postgres.BuildServices();
        var store = sp.GetRequiredService<IJobExecutionStore>();
        var old = DateTimeOffset.UtcNow.AddDays(-400);
        var runId = Guid.CreateVersion7(old);

        await store.CreateAsync(
            new JobExecutionRecord
            {
                RunId = runId,
                JobName = "legacy",
                ParametersHash = "x",
                ParametersJson = "{}",
                Trigger = JobTrigger.Test,
                HostName = "test",
                StartedAt = old,
                LastHeartbeatAt = old,
            },
            Ct);
        await store.CompleteAsync(runId, JobStatus.Succeeded, 0, null, JobCounters.Zero, old, Ct);

        var outcome = await sp.GetRequiredService<IJobRunner>().RunAsync(
            new RunRequest("purge-executions", new Dictionary<string, string?> { ["RetentionDays"] = "365" }, JobTrigger.Test),
            Ct);

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        Assert.DoesNotContain(await store.ListRecentAsync("legacy", 10, Ct), r => r.RunId == runId);
    }

    [Theory]
    [InlineData(new[] { "a=1", "b=x=y" }, true)]
    [InlineData(new[] { "novalue" }, false)]
    [InlineData(new[] { "=1" }, false)]
    [InlineData(new[] { "a=1", "A=2" }, false)]
    public void Cli_parameters_are_parsed_strictly(string[] raw, bool valid)
    {
        var ok = CliCommands.TryParseParameters(raw, out var parameters, out var error);

        Assert.Equal(valid, ok);
        if (valid)
        {
            Assert.Equal("x=y", parameters["b"]);
            Assert.Null(error);
        }
        else
        {
            Assert.NotNull(error);
        }
    }

    private static async Task<string> WriteCsvAsync(params string[] lines)
    {
        var path = Path.Combine(Path.GetTempPath(), $"invoices-{Guid.NewGuid():N}.csv");
        await File.WriteAllLinesAsync(path, lines, Ct);
        return path;
    }
}
