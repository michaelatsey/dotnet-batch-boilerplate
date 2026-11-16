using Batch.Abstractions;

namespace Batch.Core.Persistence;

/// <summary>Une ligne du journal d'exécution : qui, quoi, quand, où, combien, et où reprendre.</summary>
public sealed record JobExecutionRecord
{
    public required Guid RunId { get; init; }

    public required string JobName { get; init; }

    public required string ParametersHash { get; init; }

    public required string ParametersJson { get; init; }

    public required JobTrigger Trigger { get; init; }

    public required string HostName { get; init; }

    public required DateTimeOffset StartedAt { get; init; }

    public required DateTimeOffset LastHeartbeatAt { get; init; }

    public Guid? RestartOf { get; init; }

    public JobStatus Status { get; init; } = JobStatus.Running;

    public DateTimeOffset? EndedAt { get; init; }

    /// <summary>Dernière position commitée. Point de départ d'une reprise.</summary>
    public string? Checkpoint { get; init; }

    public long ReadCount { get; init; }

    public long WriteCount { get; init; }

    public long SkipCount { get; init; }

    public int? ExitCode { get; init; }

    public string? Message { get; init; }

    public JobCounters Counters => new(ReadCount, WriteCount, SkipCount);
}

/// <summary>Journal d'exécution. Source de vérité pour la reprise, l'audit et la supervision.</summary>
public interface IJobExecutionStore
{
    /// <summary>
    /// Passe en <see cref="JobStatus.Abandoned"/> les exécutions restées « Running » pour cette clé.
    /// Appelé une fois le verrou obtenu : si le verrou est libre, leur processus est forcément mort.
    /// <paramref name="parametersHash"/> à <c>null</c> vise toutes les exécutions du job.
    /// </summary>
    Task<int> MarkAbandonedAsync(string jobName, string? parametersHash, DateTimeOffset now, CancellationToken cancellationToken);

    Task<JobExecutionRecord?> FindLatestAsync(string jobName, string parametersHash, CancellationToken cancellationToken);

    Task CreateAsync(JobExecutionRecord record, CancellationToken cancellationToken);

    Task HeartbeatAsync(Guid runId, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken);

    Task SaveCheckpointAsync(Guid runId, string checkpoint, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken);

    Task CompleteAsync(Guid runId, JobStatus status, int exitCode, string? message, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken);

    Task<IReadOnlyList<JobExecutionRecord>> ListRecentAsync(string? jobName, int take, CancellationToken cancellationToken);

    /// <summary>Supprime les exécutions terminées avant <paramref name="endedBefore"/>.</summary>
    Task<int> PurgeAsync(DateTimeOffset endedBefore, CancellationToken cancellationToken);
}
