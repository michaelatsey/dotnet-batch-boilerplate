using Batch.Abstractions;
using Microsoft.Extensions.Logging;

namespace Batch.Core.Execution;

/// <summary>Tout ce que le moteur sait d'une exécution en cours. Interne : le job ne voit que <see cref="JobContext{TParams}"/>.</summary>
internal sealed class RunInvocation
{
    public required Guid RunId { get; init; }

    public required string JobName { get; init; }

    public required object Parameters { get; init; }

    public required JobTrigger Trigger { get; init; }

    public Guid? RestartOf { get; init; }

    /// <summary>Checkpoint de l'exécution reprise. <c>null</c> pour un démarrage à neuf.</summary>
    public string? ResumeCheckpoint { get; init; }

    public required BatchJobOptions Options { get; init; }

    public required JobProgress Progress { get; init; }

    public required ILogger Logger { get; init; }

    public required Func<string, CancellationToken, Task> SaveCheckpointAsync { get; init; }

    public JobContext<TParams> CreateContext<TParams>()
        where TParams : class, new() => new()
        {
            RunId = RunId,
            JobName = JobName,
            Parameters = (TParams)Parameters,
            Trigger = Trigger,
            RestartOf = RestartOf,
            Progress = Progress,
            Logger = Logger,
        };
}

internal sealed class JobProgress : IJobProgress
{
    private long _read;
    private long _written;
    private long _skipped;

    public void AddRead(long count = 1) => Interlocked.Add(ref _read, count);

    public void AddWritten(long count = 1) => Interlocked.Add(ref _written, count);

    public void AddSkipped(long count = 1) => Interlocked.Add(ref _skipped, count);

    public JobCounters Snapshot() => new(
        Interlocked.Read(ref _read),
        Interlocked.Read(ref _written),
        Interlocked.Read(ref _skipped));
}
