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
    public async Task Out_of_range_amount_is_skipped_not_a_failed_chunk()
    {
        var file = await WriteCsvAsync(
            "invoice_number;customer_id;issue_date;amount;currency",
            "T3-0001;C1;2026-01-01;10.00;EUR",
            "T3-0002;C2;2026-01-02;1000000000000.00;EUR"); // dépasse numeric(14,2)

        await using var sp = postgres.BuildServices();
        var outcome = await sp.GetRequiredService<IJobRunner>().RunAsync(
            new RunRequest("import-invoices", new Dictionary<string, string?> { ["InputFile"] = file }, JobTrigger.Test),
            Ct);

        Assert.Equal(ExitCodes.CompletedWithWarnings, outcome.ExitCode);
        Assert.Equal(new JobCounters(2, 1, 1), outcome.Counters);
        Assert.Equal(1, await postgres.ScalarAsync("SELECT count(*) FROM billing.invoice WHERE invoice_number LIKE 'T3-%'"));
    }

    [Fact]
    public async Task Data_rejected_by_the_database_is_a_functional_error_not_a_retry_loop()
    {
        // Une contrainte que la validation du job ignore (ajoutée par un DBA, par exemple).
        await postgres.ScalarAsync("ALTER TABLE billing.invoice ADD CONSTRAINT ck_test_forbidden CHECK (customer_id <> 'FORBIDDEN') NOT VALID; SELECT 0");
        try
        {
            var file = await WriteCsvAsync(
                "invoice_number;customer_id;issue_date;amount;currency",
                "T4-0001;FORBIDDEN;2026-01-01;10.00;EUR");

            await using var sp = postgres.BuildServices();
            var outcome = await sp.GetRequiredService<IJobRunner>().RunAsync(
                new RunRequest("import-invoices", new Dictionary<string, string?> { ["InputFile"] = file }, JobTrigger.Test),
                Ct);

            // 2 : relancer ne changera rien, il faut corriger la donnée ou la contrainte.
            Assert.Equal(ExitCodes.FunctionalError, outcome.ExitCode);
            Assert.Contains("ck_test_forbidden", outcome.Message, StringComparison.Ordinal);
        }
        finally
        {
            await postgres.ScalarAsync("ALTER TABLE billing.invoice DROP CONSTRAINT ck_test_forbidden; SELECT 0");
        }
    }

    [Theory]
    [InlineData("40P01", true)]  // deadlock_detected : transitoire pour Npgsql, donc retenté
    [InlineData("08006", true)]  // connection_failure : coupure de connexion, retentée aussi
    [InlineData("57014", false)] // query_canceled (statement_timeout) : ni donnée ni contrainte
    public async Task Non_data_database_error_stays_a_technical_error(string sqlState, bool retried)
    {
        // Seules les classes 22 (donnée) et 23 (contrainte) deviennent fonctionnelles (2).
        // Tout le reste garde le code 3, donc la relance automatique de l'ordonnanceur.
        await postgres.ScalarAsync(
            """
            CREATE SEQUENCE IF NOT EXISTS test_write_attempts;
            CREATE OR REPLACE FUNCTION test_raise_sqlstate() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM nextval('test_write_attempts'); -- hors transaction : compte chaque tentative
                RAISE EXCEPTION 'simulated %', substr(NEW.customer_id, 7) USING ERRCODE = substr(NEW.customer_id, 7);
            END $$;
            CREATE TRIGGER test_raise_sqlstate BEFORE INSERT ON billing.invoice
                FOR EACH ROW WHEN (NEW.customer_id LIKE 'RAISE-%') EXECUTE FUNCTION test_raise_sqlstate();
            SELECT setval('test_write_attempts', 1, false);
            """);
        try
        {
            var file = await WriteCsvAsync(
                "invoice_number;customer_id;issue_date;amount;currency",
                $"T5-{sqlState};RAISE-{sqlState};2026-01-01;10.00;EUR");

            await using var sp = postgres.BuildServices(new()
            {
                ["Batch:Retry:MaxAttempts"] = "2",
                ["Batch:Retry:BaseDelay"] = "00:00:00.010",
                ["Batch:Retry:MaxDelay"] = "00:00:00.050",
            });
            var outcome = await sp.GetRequiredService<IJobRunner>().RunAsync(
                new RunRequest("import-invoices", new Dictionary<string, string?> { ["InputFile"] = file }, JobTrigger.Test),
                Ct);

            Assert.Equal(ExitCodes.TechnicalError, outcome.ExitCode);
            Assert.Contains(sqlState, outcome.Message, StringComparison.Ordinal);

            // 1 tentative + 2 retries quand l'erreur est transitoire ; 1 seule sinon.
            var attempts = await postgres.ScalarAsync("SELECT last_value FROM test_write_attempts");
            Assert.Equal(retried ? 3 : 1, attempts);
        }
        finally
        {
            await postgres.ScalarAsync(
                "DROP TRIGGER IF EXISTS test_raise_sqlstate ON billing.invoice; DROP FUNCTION IF EXISTS test_raise_sqlstate(); SELECT 0");
        }
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
