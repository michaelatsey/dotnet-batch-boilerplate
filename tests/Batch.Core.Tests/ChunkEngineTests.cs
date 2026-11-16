using Batch.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using static Batch.Core.Tests.TestSupport;

namespace Batch.Core.Tests;

public sealed class ChunkEngineTests
{
    private static ServiceProvider BuildNumbers(
        NumbersScript script,
        Action<BatchJobOptions>? configure = null,
        TimeProvider? time = null,
        Dictionary<string, string?>? configuration = null) =>
        Build(
            b => b.AddChunkedJob<NumbersJob, RangeParameters, int, NumberRow>("numbers", o =>
            {
                o.ChunkSize = 10;
                configure?.Invoke(o);
            }),
            time,
            configuration,
            s => s.AddSingleton(script));

    private static Dictionary<string, string?> Count(int n) => new() { ["Count"] = n.ToString(System.Globalization.CultureInfo.InvariantCulture) };

    [Fact]
    public async Task Writes_every_item_in_read_order_by_chunks()
    {
        var script = new NumbersScript();
        await using var sp = BuildNumbers(script);

        var outcome = await sp.RunAsync("numbers", Count(95));

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        Assert.Equal(Enumerable.Range(1, 95), script.Written);
        int[] expectedChunks = [10, 10, 10, 10, 10, 10, 10, 10, 10, 5];
        Assert.Equal(expectedChunks, script.Chunks);
        Assert.Equal(new JobCounters(95, 95, 0), outcome.Counters);

        var record = await sp.SingleExecutionAsync("numbers");
        Assert.Equal("95", record.Checkpoint);
    }

    [Fact]
    public async Task Parallel_processing_preserves_output_order()
    {
        var script = new NumbersScript { Delay = TimeSpan.FromMilliseconds(1) };
        await using var sp = BuildNumbers(script, o => o.MaxDegreeOfParallelism = 8);

        var outcome = await sp.RunAsync("numbers", Count(200));

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        Assert.Equal(Enumerable.Range(1, 200), script.Written);
    }

    [Fact]
    public async Task Filtered_items_are_not_written_and_not_counted_as_skipped()
    {
        var script = new NumbersScript();
        script.FilteredItems.UnionWith([3, 4, 5]);
        await using var sp = BuildNumbers(script);

        var outcome = await sp.RunAsync("numbers", Count(20));

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        Assert.Equal(new JobCounters(20, 17, 0), outcome.Counters);
    }

    [Fact]
    public async Task Skips_under_the_limit_complete_with_warnings()
    {
        var script = new NumbersScript();
        script.InvalidItems.UnionWith([7, 42]);
        await using var sp = BuildNumbers(script, o => o.SkipLimit = 5);

        var outcome = await sp.RunAsync("numbers", Count(50));

        Assert.Equal(ExitCodes.CompletedWithWarnings, outcome.ExitCode);
        Assert.Equal(new JobCounters(50, 48, 2), outcome.Counters);
        Assert.DoesNotContain(7, script.Written);
    }

    [Fact]
    public async Task Skips_over_the_limit_fail_functionally()
    {
        var script = new NumbersScript();
        script.InvalidItems.UnionWith([1, 2, 3]);
        await using var sp = BuildNumbers(script, o => o.SkipLimit = 2);

        var outcome = await sp.RunAsync("numbers", Count(50));

        Assert.Equal(ExitCodes.FunctionalError, outcome.ExitCode);
        Assert.Contains("Skip limit exceeded", outcome.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Restart_resumes_after_the_last_committed_chunk()
    {
        var script = new NumbersScript { CrashAt = 25 };
        await using var sp = BuildNumbers(script);

        var failed = await sp.RunAsync("numbers", Count(50));
        Assert.Equal(ExitCodes.TechnicalError, failed.ExitCode);
        Assert.Equal(Enumerable.Range(1, 20), script.Written);

        script.CrashAt = null;
        var resumed = await sp.RunAsync("numbers", Count(50), restart: true);

        Assert.Equal(ExitCodes.Success, resumed.ExitCode);
        Assert.Equal("20", script.ResumeAfter);
        Assert.Equal(Enumerable.Range(1, 50), script.Written);
        Assert.Equal(30, resumed.Counters.Written);
    }

    [Fact]
    public async Task Restart_after_success_is_a_no_op()
    {
        var script = new NumbersScript();
        await using var sp = BuildNumbers(script);

        await sp.RunAsync("numbers", Count(10));
        var again = await sp.RunAsync("numbers", Count(10), restart: true);

        Assert.Equal(ExitCodes.Success, again.ExitCode);
        Assert.Equal(10, script.Written.Count);
    }

    [Fact]
    public async Task Restart_starts_fresh_when_the_input_changed_since_the_failed_run()
    {
        var script = new NumbersScript { CrashAt = 25 };
        await using var sp = BuildNumbers(script);

        await sp.RunAsync("numbers", Count(50));

        // Un autre fichier a été déposé au même chemin : le checkpoint « 20 » ne veut plus rien dire.
        script.CrashAt = null;
        script.InputIdentity = "v2";
        var restarted = await sp.RunAsync("numbers", Count(50), restart: true);

        Assert.Equal(ExitCodes.Success, restarted.ExitCode);
        Assert.Null(script.ResumeAfter);
        Assert.Equal(50, restarted.Counters.Written);

        var journal = await sp.GetRequiredService<Batch.Core.Persistence.IJobExecutionStore>().ListRecentAsync("numbers", 10, Ct);
        var latest = journal.Single(r => r.RunId == restarted.RunId);
        Assert.Equal("v2", latest.InputIdentity);
        Assert.Null(latest.RestartOf);
    }

    [Fact]
    public async Task Restart_after_success_runs_again_when_the_input_changed()
    {
        var script = new NumbersScript();
        await using var sp = BuildNumbers(script);

        await sp.RunAsync("numbers", Count(10));
        script.InputIdentity = "v2";
        var again = await sp.RunAsync("numbers", Count(10), restart: true);

        Assert.Equal(ExitCodes.Success, again.ExitCode);
        Assert.NotNull(again.RunId);
        Assert.Equal(20, script.Written.Count);
    }

    [Fact]
    public async Task Restart_starts_fresh_when_the_input_identity_cannot_be_computed()
    {
        var script = new NumbersScript { CrashAt = 25 };
        await using var sp = BuildNumbers(script);

        await sp.RunAsync("numbers", Count(50));

        // Sans preuve que l'entrée est la même, on ne reprend pas : rejouer est sûr, sauter des lignes ne l'est pas.
        script.CrashAt = null;
        script.InputIdentityFailure = new IOException("NFS hiccup");
        var restarted = await sp.RunAsync("numbers", Count(50), restart: true);

        Assert.Equal(ExitCodes.Success, restarted.ExitCode);
        Assert.Null(script.ResumeAfter);
        Assert.Equal(50, restarted.Counters.Written);
    }

    [Fact]
    public async Task Restart_finds_the_failed_run_when_a_default_is_spelled_out()
    {
        var script = new NumbersScript { CrashAt = 25 };
        await using var sp = BuildNumbers(script);

        await sp.RunAsync("numbers"); // Count = 100 par défaut

        script.CrashAt = null;
        var resumed = await sp.RunAsync("numbers", Count(100), restart: true);

        Assert.Equal(ExitCodes.Success, resumed.ExitCode);
        Assert.Equal("20", script.ResumeAfter);
    }

    [Fact]
    public async Task Transient_write_failure_is_retried()
    {
        var script = new NumbersScript { FailWriteOnce = new TimeoutException("db timeout") };
        await using var sp = BuildNumbers(
            script,
            time: TimeProvider.System,
            configuration: new()
            {
                ["Batch:Retry:MaxAttempts"] = "2",
                ["Batch:Retry:BaseDelay"] = "00:00:00.010",
            });

        var outcome = await sp.RunAsync("numbers", Count(30));

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        Assert.Equal(Enumerable.Range(1, 30), script.Written);
    }

    [Fact]
    public async Task Non_transient_write_failure_is_not_retried()
    {
        var script = new NumbersScript { FailWriteOnce = new InvalidOperationException("constraint violation") };
        await using var sp = BuildNumbers(
            script,
            time: TimeProvider.System,
            configuration: new() { ["Batch:Retry:MaxAttempts"] = "2", ["Batch:Retry:BaseDelay"] = "00:00:00.010" });

        var outcome = await sp.RunAsync("numbers", Count(30));

        Assert.Equal(ExitCodes.TechnicalError, outcome.ExitCode);
        Assert.Empty(script.Written);
    }

    [Fact]
    public async Task Configuration_overrides_code_defaults()
    {
        var script = new NumbersScript();
        await using var sp = BuildNumbers(script, configuration: new() { ["Batch:Jobs:numbers:ChunkSize"] = "25" });

        await sp.RunAsync("numbers", Count(50));

        int[] expectedChunks = [25, 25];
        Assert.Equal(expectedChunks, script.Chunks);
    }
}
