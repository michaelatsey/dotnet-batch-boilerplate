using Batch.Abstractions;

namespace Batch.Core.Persistence;

/// <summary>Journal en mémoire : développement local et tests. Perdu à l'arrêt du processus.</summary>
public sealed class InMemoryJobExecutionStore : IJobExecutionStore
{
    private readonly Lock _gate = new();
    private readonly Dictionary<Guid, JobExecutionRecord> _records = [];

    public Task<int> MarkAbandonedAsync(string jobName, string? parametersHash, DateTimeOffset now, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var zombies = _records.Values
                .Where(r => r.JobName == jobName
                            && r.Status == JobStatus.Running
                            && (parametersHash is null || r.ParametersHash == parametersHash))
                .ToList();

            foreach (var zombie in zombies)
            {
                _records[zombie.RunId] = zombie with
                {
                    Status = JobStatus.Abandoned,
                    EndedAt = now,
                    Message = "Process died without finalizing (detected at next lock acquisition).",
                };
            }

            return Task.FromResult(zombies.Count);
        }
    }

    public Task<JobExecutionRecord?> FindLatestAsync(string jobName, string parametersHash, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var latest = _records.Values
                .Where(r => r.JobName == jobName && r.ParametersHash == parametersHash)
                .MaxBy(r => r.StartedAt);
            return Task.FromResult(latest);
        }
    }

    public Task CreateAsync(JobExecutionRecord record, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _records.Add(record.RunId, record);
        }

        return Task.CompletedTask;
    }

    public Task HeartbeatAsync(Guid runId, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken) =>
        Update(runId, r => r with
        {
            LastHeartbeatAt = now,
            ReadCount = counters.Read,
            WriteCount = counters.Written,
            SkipCount = counters.Skipped,
        });

    public Task SaveCheckpointAsync(Guid runId, string checkpoint, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken) =>
        Update(runId, r => r with
        {
            Checkpoint = checkpoint,
            LastHeartbeatAt = now,
            ReadCount = counters.Read,
            WriteCount = counters.Written,
            SkipCount = counters.Skipped,
        });

    public Task CompleteAsync(Guid runId, JobStatus status, int exitCode, string? message, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken) =>
        Update(runId, r => r with
        {
            Status = status,
            ExitCode = exitCode,
            Message = message,
            EndedAt = now,
            LastHeartbeatAt = now,
            ReadCount = counters.Read,
            WriteCount = counters.Written,
            SkipCount = counters.Skipped,
        });

    public Task<IReadOnlyList<JobExecutionRecord>> ListRecentAsync(string? jobName, int take, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            IReadOnlyList<JobExecutionRecord> result = _records.Values
                .Where(r => jobName is null || r.JobName == jobName)
                .OrderByDescending(r => r.StartedAt)
                .Take(take)
                .ToList();
            return Task.FromResult(result);
        }
    }

    public Task<int> PurgeAsync(DateTimeOffset endedBefore, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var expired = _records.Values.Where(r => r.EndedAt < endedBefore).Select(r => r.RunId).ToList();
            foreach (var runId in expired)
            {
                _records.Remove(runId);
            }

            return Task.FromResult(expired.Count);
        }
    }

    private Task Update(Guid runId, Func<JobExecutionRecord, JobExecutionRecord> change)
    {
        lock (_gate)
        {
            if (_records.TryGetValue(runId, out var record))
            {
                _records[runId] = change(record);
            }
        }

        return Task.CompletedTask;
    }
}
