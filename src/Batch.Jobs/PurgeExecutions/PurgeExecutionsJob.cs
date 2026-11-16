using System.ComponentModel.DataAnnotations;
using Batch.Abstractions;
using Batch.Core.Persistence;

namespace Batch.Jobs.PurgeExecutions;

/// <summary><c>batch run purge-executions -p RetentionDays=90</c></summary>
public sealed class PurgeExecutionsParameters
{
    [Range(1, 3650)]
    public int RetentionDays { get; set; } = 90;
}

/// <summary>Job « tâche » minimal : le moteur fournit verrou, journal, timeout et codes retour.</summary>
public sealed class PurgeExecutionsJob(IJobExecutionStore store, TimeProvider time) : IBatchJob<PurgeExecutionsParameters>
{
    public async Task<JobResult> ExecuteAsync(JobContext<PurgeExecutionsParameters> context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var threshold = time.GetUtcNow().AddDays(-context.Parameters.RetentionDays);
        var deleted = await store.PurgeAsync(threshold, cancellationToken);

        return deleted == 0
            ? JobResult.NothingToDo($"No execution ended before {threshold:O}.")
            : JobResult.Success($"Purged {deleted} execution(s) ended before {threshold:O}.");
    }
}
