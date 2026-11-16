using System.ComponentModel.DataAnnotations;
using System.Runtime.CompilerServices;
using Batch.Abstractions;
using Batch.Core.Execution;
using Batch.Core.Persistence;
using Batch.Core.Registration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Batch.Core.Tests;

internal static class TestSupport
{
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static ServiceProvider Build(
        Action<BatchBuilder> jobs,
        TimeProvider? time = null,
        Dictionary<string, string?>? configuration = null,
        Action<IServiceCollection>? services = null)
    {
        var collection = new ServiceCollection();

        var settings = new Dictionary<string, string?>
        {
            // Par défaut, pas de retry : les tests de retry l'activent explicitement.
            ["Batch:Retry:MaxAttempts"] = "0",
        };
        foreach (var (key, value) in configuration ?? [])
        {
            settings[key] = value;
        }

        collection.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
        collection.AddLogging();
        collection.AddSingleton(time ?? new FakeTimeProvider());

        var builder = collection.AddBatch();
        jobs(builder);
        services?.Invoke(collection);

        return collection.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }

    public static Task<RunOutcome> RunAsync(
        this IServiceProvider services,
        string job,
        Dictionary<string, string?>? parameters = null,
        bool restart = false,
        CancellationToken? cancellationToken = null) =>
        services.GetRequiredService<IJobRunner>().RunAsync(
            new RunRequest(job, parameters ?? [], JobTrigger.Test, restart),
            cancellationToken ?? Ct);

    public static async Task<JobExecutionRecord> SingleExecutionAsync(this IServiceProvider services, string job)
    {
        var records = await services.GetRequiredService<IJobExecutionStore>().ListRecentAsync(job, 10, Ct);
        return Assert.Single(records);
    }
}

// ── Jobs de test ───────────────────────────────────────────────────────────────

public sealed class EchoParameters
{
    [Required]
    public string Name { get; set; } = "";

    public int Count { get; set; } = 1;

    public DateOnly? Date { get; set; }

    public DayOfWeek Day { get; set; } = DayOfWeek.Monday;
}

/// <summary>Job « tâche » dont le comportement est piloté par le test.</summary>
public sealed class ScriptedJob(ScriptedBehavior behavior) : IBatchJob<EchoParameters>
{
    public Task<JobResult> ExecuteAsync(JobContext<EchoParameters> context, CancellationToken cancellationToken) =>
        behavior.Run(context, cancellationToken);
}

public sealed class ScriptedBehavior
{
    public Func<JobContext<EchoParameters>, CancellationToken, Task<JobResult>> Run { get; set; } =
        (_, _) => Task.FromResult(JobResult.Success());
}

public sealed class RangeParameters
{
    public int Count { get; set; } = 100;
}

/// <summary>Job par chunks sur une séquence d'entiers 1..Count, avec points d'injection de pannes.</summary>
public sealed class NumbersJob(NumbersScript script)
    : IChunkedJob<RangeParameters, int, NumberRow>, IInputIdentityProvider<RangeParameters>
{
    public ValueTask<string> ComputeInputIdentityAsync(RangeParameters parameters, CancellationToken cancellationToken) =>
        script.InputIdentityFailure is { } failure
            ? ValueTask.FromException<string>(failure)
            : ValueTask.FromResult(script.InputIdentity);

    public async IAsyncEnumerable<int> ReadAsync(
        JobContext<RangeParameters> context,
        string? resumeAfter,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        script.ResumeAfter = resumeAfter;
        var start = resumeAfter is null ? 1 : int.Parse(resumeAfter, System.Globalization.CultureInfo.InvariantCulture) + 1;
        for (var i = start; i <= context.Parameters.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return i;
        }
    }

    public async ValueTask<NumberRow?> ProcessAsync(int item, JobContext<RangeParameters> context, CancellationToken cancellationToken)
    {
        if (script.Delay is { } delay)
        {
            await Task.Delay(delay, cancellationToken);
        }

        if (script.InvalidItems.Contains(item))
        {
            throw new SkippableItemException($"{item} is invalid");
        }

        if (script.FilteredItems.Contains(item))
        {
            return null;
        }

        return new NumberRow(item, item * item);
    }

    public Task WriteAsync(IReadOnlyList<NumberRow> chunk, JobContext<RangeParameters> context, CancellationToken cancellationToken)
    {
        if (script.FailWriteOnce is { } failure)
        {
            script.FailWriteOnce = null;
            throw failure;
        }

        if (chunk.Any(r => r.Value == script.CrashAt))
        {
            throw new InvalidOperationException($"Simulated crash at {script.CrashAt}");
        }

        lock (script.Written)
        {
            script.Chunks.Add(chunk.Count);
            script.Written.AddRange(chunk.Select(r => r.Value));
        }

        return Task.CompletedTask;
    }

    public string CheckpointOf(int item) => item.ToString(System.Globalization.CultureInfo.InvariantCulture);
}

public sealed record NumberRow(int Value, int Square);

public sealed class NumbersScript
{
    public List<int> Written { get; } = [];

    public List<int> Chunks { get; } = [];

    public HashSet<int> InvalidItems { get; } = [];

    public HashSet<int> FilteredItems { get; } = [];

    public int? CrashAt { get; set; }

    public Exception? FailWriteOnce { get; set; }

    public TimeSpan? Delay { get; set; }

    public string? ResumeAfter { get; set; }

    /// <summary>Identité de la « source » : la changer simule un nouveau fichier déposé au même chemin.</summary>
    public string InputIdentity { get; set; } = "v1";

    public Exception? InputIdentityFailure { get; set; }
}
