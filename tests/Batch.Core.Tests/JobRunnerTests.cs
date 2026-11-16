using Batch.Abstractions;
using Batch.Core.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using static Batch.Core.Tests.TestSupport;

namespace Batch.Core.Tests;

/// <summary>Le contrat avec l'ordonnanceur : chaque situation produit son code retour, et le journal le reflète.</summary>
public sealed class JobRunnerTests
{
    private static readonly Dictionary<string, string?> ValidParameters = new() { ["Name"] = "alice" };

    private static ServiceProvider BuildScripted(ScriptedBehavior behavior, TimeProvider? time = null, Action<BatchJobOptions>? configure = null) =>
        Build(
            b => b.AddJob<ScriptedJob, EchoParameters>("scripted", configure),
            time,
            services: s => s.AddSingleton(behavior));

    [Fact]
    public async Task Unknown_job_returns_invalid_usage()
    {
        await using var sp = BuildScripted(new ScriptedBehavior());

        var outcome = await sp.RunAsync("does-not-exist");

        Assert.Equal(ExitCodes.InvalidUsage, outcome.ExitCode);
        Assert.Null(outcome.RunId);
    }

    [Theory]
    [InlineData(null, null)]          // Name manquant ([Required])
    [InlineData("Count", "abc")]      // conversion impossible
    [InlineData("Typo", "x")]         // clé inconnue : faute de frappe dans l'ordonnanceur
    public async Task Invalid_parameters_return_invalid_usage_before_any_side_effect(string? key, string? value)
    {
        await using var sp = BuildScripted(new ScriptedBehavior());
        var parameters = key is null ? [] : new Dictionary<string, string?> { ["Name"] = "alice", [key] = value };

        var outcome = await sp.RunAsync("scripted", parameters);

        Assert.Equal(ExitCodes.InvalidUsage, outcome.ExitCode);
        var journal = await sp.GetRequiredService<IJobExecutionStore>().ListRecentAsync(null, 10, Ct);
        Assert.Empty(journal);
    }

    [Fact]
    public async Task Success_is_journaled_with_exit_code_zero()
    {
        await using var sp = BuildScripted(new ScriptedBehavior());

        var outcome = await sp.RunAsync("scripted", ValidParameters);

        Assert.Equal(ExitCodes.Success, outcome.ExitCode);
        var record = await sp.SingleExecutionAsync("scripted");
        Assert.Equal(JobStatus.Succeeded, record.Status);
        Assert.Equal(0, record.ExitCode);
        Assert.NotNull(record.EndedAt);
    }

    [Fact]
    public async Task Parameters_are_bound_with_typed_conversions()
    {
        EchoParameters? seen = null;
        var behavior = new ScriptedBehavior
        {
            Run = (ctx, _) =>
            {
                seen = ctx.Parameters;
                return Task.FromResult(JobResult.Success());
            },
        };
        await using var sp = BuildScripted(behavior);

        await sp.RunAsync("scripted", new() { ["name"] = "bob", ["COUNT"] = "3", ["Date"] = "2026-11-13", ["Day"] = "Friday" });

        Assert.NotNull(seen);
        Assert.Equal("bob", seen.Name);
        Assert.Equal(3, seen.Count);
        Assert.Equal(new DateOnly(2026, 11, 13), seen.Date);
        Assert.Equal(DayOfWeek.Friday, seen.Day);
    }

    [Fact]
    public async Task Functional_error_returns_2()
    {
        var behavior = new ScriptedBehavior { Run = (_, _) => throw new BatchFunctionalException("bad data") };
        await using var sp = BuildScripted(behavior);

        var outcome = await sp.RunAsync("scripted", ValidParameters);

        Assert.Equal(ExitCodes.FunctionalError, outcome.ExitCode);
        Assert.Equal(JobStatus.FunctionalError, (await sp.SingleExecutionAsync("scripted")).Status);
    }

    [Fact]
    public async Task Technical_error_returns_3()
    {
        var behavior = new ScriptedBehavior { Run = (_, _) => throw new InvalidOperationException("boom") };
        await using var sp = BuildScripted(behavior);

        var outcome = await sp.RunAsync("scripted", ValidParameters);

        Assert.Equal(ExitCodes.TechnicalError, outcome.ExitCode);
        Assert.Contains("boom", outcome.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Second_run_with_same_parameters_is_rejected_while_first_is_running()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var behavior = new ScriptedBehavior
        {
            Run = async (_, ct) =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(ct);
                return JobResult.Success();
            },
        };
        await using var sp = BuildScripted(behavior);

        var first = sp.RunAsync("scripted", ValidParameters);
        await started.Task.WaitAsync(Ct);

        var second = await sp.RunAsync("scripted", ValidParameters);
        release.SetResult();

        Assert.Equal(ExitCodes.AlreadyRunning, second.ExitCode);
        Assert.Equal(ExitCodes.Success, (await first).ExitCode);
    }

    [Fact]
    public async Task Runs_with_different_parameters_proceed_in_parallel()
    {
        var running = 0;
        var bothRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var behavior = new ScriptedBehavior
        {
            Run = async (_, ct) =>
            {
                if (Interlocked.Increment(ref running) == 2)
                {
                    bothRunning.TrySetResult();
                }

                await bothRunning.Task.WaitAsync(ct);
                return JobResult.Success();
            },
        };
        await using var sp = BuildScripted(behavior);

        var a = sp.RunAsync("scripted", new() { ["Name"] = "a" });
        var b = sp.RunAsync("scripted", new() { ["Name"] = "b" });
        var outcomes = await Task.WhenAll(a, b).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.All(outcomes, o => Assert.Equal(ExitCodes.Success, o.ExitCode));
    }

    [Fact]
    public async Task Single_instance_mode_rejects_even_with_different_parameters()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var behavior = new ScriptedBehavior
        {
            Run = async (_, ct) =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(ct);
                return JobResult.Success();
            },
        };
        await using var sp = BuildScripted(behavior, configure: o => o.Concurrency = ConcurrencyMode.SingleInstance);

        var first = sp.RunAsync("scripted", new() { ["Name"] = "a" });
        await started.Task.WaitAsync(Ct);
        var second = await sp.RunAsync("scripted", new() { ["Name"] = "b" });
        release.SetResult();

        Assert.Equal(ExitCodes.AlreadyRunning, second.ExitCode);
        Assert.Equal(ExitCodes.Success, (await first).ExitCode);
    }

    [Fact]
    public async Task Host_cancellation_returns_5()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var behavior = new ScriptedBehavior
        {
            Run = async (_, ct) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return JobResult.Success();
            },
        };
        await using var sp = BuildScripted(behavior);
        using var sigterm = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var run = sp.RunAsync("scripted", ValidParameters, cancellationToken: sigterm.Token);
        await started.Task.WaitAsync(Ct);
        await sigterm.CancelAsync();

        var outcome = await run;
        Assert.Equal(ExitCodes.Cancelled, outcome.ExitCode);
        Assert.Equal(JobStatus.Cancelled, (await sp.SingleExecutionAsync("scripted")).Status);
    }

    [Fact]
    public async Task Timeout_returns_6_using_the_injected_clock()
    {
        var time = new FakeTimeProvider();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var behavior = new ScriptedBehavior
        {
            Run = async (_, ct) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return JobResult.Success();
            },
        };
        await using var sp = BuildScripted(behavior, time, o => o.Timeout = TimeSpan.FromMinutes(10));

        var run = sp.RunAsync("scripted", ValidParameters);
        await started.Task.WaitAsync(Ct);
        time.Advance(TimeSpan.FromMinutes(11));

        var outcome = await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(ExitCodes.TimedOut, outcome.ExitCode);
    }

    [Fact]
    public async Task Stale_running_execution_is_marked_abandoned_when_the_lock_is_free()
    {
        await using var sp = BuildScripted(new ScriptedBehavior());
        var store = sp.GetRequiredService<IJobExecutionStore>();

        // Un premier run réussi donne l'empreinte des paramètres ; on simule ensuite un processus tué net.
        await sp.RunAsync("scripted", ValidParameters);
        var reference = await sp.SingleExecutionAsync("scripted");
        var zombie = reference with { RunId = Guid.CreateVersion7(), Status = JobStatus.Running, EndedAt = null, ExitCode = null };
        await store.CreateAsync(zombie, Ct);

        await sp.RunAsync("scripted", ValidParameters);

        var all = await store.ListRecentAsync("scripted", 10, Ct);
        Assert.Equal(JobStatus.Abandoned, all.Single(r => r.RunId == zombie.RunId).Status);
    }
}
