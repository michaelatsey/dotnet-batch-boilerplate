using Batch.Abstractions;
using Batch.Core.Persistence;
using Microsoft.EntityFrameworkCore;

namespace Batch.Infrastructure.PostgreSql;

/// <summary>
/// Journal PostgreSQL. Un DbContext court par opération (fabrique poolée) : le heartbeat et le checkpoint
/// peuvent écrire en parallèle sans partager de contexte. Les mises à jour passent par
/// <c>ExecuteUpdateAsync</c> : une requête UPDATE, aucun chargement d'entité.
/// </summary>
internal sealed class EfJobExecutionStore(IDbContextFactory<BatchDbContext> factory) : IJobExecutionStore
{
    private const string AbandonedMessage = "Process died without finalizing (detected at next lock acquisition).";

    public async Task<int> MarkAbandonedAsync(string jobName, string? parametersHash, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        var running = db.Executions.Where(e => e.JobName == jobName && e.Status == JobStatus.Running);
        if (parametersHash is not null)
        {
            running = running.Where(e => e.ParametersHash == parametersHash);
        }

        return await running.ExecuteUpdateAsync(
            s => s
                .SetProperty(e => e.Status, JobStatus.Abandoned)
                .SetProperty(e => e.EndedAt, now)
                .SetProperty(e => e.Message, AbandonedMessage),
            cancellationToken);
    }

    public async Task<JobExecutionRecord?> FindLatestAsync(string jobName, string parametersHash, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        var entity = await db.Executions
            .AsNoTracking()
            .Where(e => e.JobName == jobName && e.ParametersHash == parametersHash)
            .OrderByDescending(e => e.StartedAt)
            .FirstOrDefaultAsync(cancellationToken);

        return entity is null ? null : ToRecord(entity);
    }

    public async Task CreateAsync(JobExecutionRecord record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        db.Executions.Add(new JobExecutionEntity
        {
            RunId = record.RunId,
            JobName = record.JobName,
            ParametersHash = record.ParametersHash,
            ParametersJson = record.ParametersJson,
            Trigger = record.Trigger,
            HostName = record.HostName,
            Status = record.Status,
            StartedAt = record.StartedAt,
            LastHeartbeatAt = record.LastHeartbeatAt,
            RestartOf = record.RestartOf,
            Checkpoint = record.Checkpoint,
        });
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task HeartbeatAsync(Guid runId, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        await db.Executions
            .Where(e => e.RunId == runId)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(e => e.LastHeartbeatAt, now)
                    .SetProperty(e => e.ReadCount, counters.Read)
                    .SetProperty(e => e.WriteCount, counters.Written)
                    .SetProperty(e => e.SkipCount, counters.Skipped),
                cancellationToken);
    }

    public async Task SaveCheckpointAsync(Guid runId, string checkpoint, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        await db.Executions
            .Where(e => e.RunId == runId)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(e => e.Checkpoint, checkpoint)
                    .SetProperty(e => e.LastHeartbeatAt, now)
                    .SetProperty(e => e.ReadCount, counters.Read)
                    .SetProperty(e => e.WriteCount, counters.Written)
                    .SetProperty(e => e.SkipCount, counters.Skipped),
                cancellationToken);
    }

    public async Task CompleteAsync(Guid runId, JobStatus status, int exitCode, string? message, JobCounters counters, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        await db.Executions
            .Where(e => e.RunId == runId)
            .ExecuteUpdateAsync(
                s => s
                    .SetProperty(e => e.Status, status)
                    .SetProperty(e => e.ExitCode, exitCode)
                    .SetProperty(e => e.Message, message)
                    .SetProperty(e => e.EndedAt, now)
                    .SetProperty(e => e.LastHeartbeatAt, now)
                    .SetProperty(e => e.ReadCount, counters.Read)
                    .SetProperty(e => e.WriteCount, counters.Written)
                    .SetProperty(e => e.SkipCount, counters.Skipped),
                cancellationToken);
    }

    public async Task<IReadOnlyList<JobExecutionRecord>> ListRecentAsync(string? jobName, int take, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        var query = db.Executions.AsNoTracking();
        if (jobName is not null)
        {
            query = query.Where(e => e.JobName == jobName);
        }

        var entities = await query.OrderByDescending(e => e.StartedAt).Take(take).ToListAsync(cancellationToken);
        return entities.ConvertAll(ToRecord);
    }

    public async Task<int> PurgeAsync(DateTimeOffset endedBefore, CancellationToken cancellationToken)
    {
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        return await db.Executions
            .Where(e => e.EndedAt != null && e.EndedAt < endedBefore)
            .ExecuteDeleteAsync(cancellationToken);
    }

    private static JobExecutionRecord ToRecord(JobExecutionEntity e) => new()
    {
        RunId = e.RunId,
        JobName = e.JobName,
        ParametersHash = e.ParametersHash,
        ParametersJson = e.ParametersJson,
        Trigger = e.Trigger,
        HostName = e.HostName,
        Status = e.Status,
        StartedAt = e.StartedAt,
        EndedAt = e.EndedAt,
        LastHeartbeatAt = e.LastHeartbeatAt,
        RestartOf = e.RestartOf,
        Checkpoint = e.Checkpoint,
        ReadCount = e.ReadCount,
        WriteCount = e.WriteCount,
        SkipCount = e.SkipCount,
        ExitCode = e.ExitCode,
        Message = e.Message,
    };
}
