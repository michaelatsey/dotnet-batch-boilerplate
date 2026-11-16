using System.Diagnostics;
using Batch.Abstractions;
using Batch.Core.Locking;
using Batch.Core.Parameters;
using Batch.Core.Persistence;
using Batch.Core.Registration;
using Batch.Core.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Batch.Core.Execution;

/// <summary>Point d'entrée unique, quel que soit le déclencheur (CLI, Quartz, test).</summary>
public interface IJobRunner
{
    Task<RunOutcome> RunAsync(RunRequest request, CancellationToken cancellationToken);
}

/// <summary>Demande d'exécution, identique quel que soit le déclencheur.</summary>
/// <remarks>
/// <c>Restart</c> reprend la dernière exécution en échec de ce job avec ces paramètres, depuis son checkpoint.
/// Si la dernière exécution a réussi : rien à faire, code 0. Si aucune : démarrage à neuf.
/// </remarks>
public sealed record RunRequest(
    string JobName,
    IReadOnlyDictionary<string, string?> Parameters,
    JobTrigger Trigger,
    bool Restart = false);

public sealed record RunOutcome(int ExitCode, JobStatus? Status, Guid? RunId, string Message, JobCounters Counters)
{
    internal static RunOutcome Rejected(int exitCode, string message) => new(exitCode, null, null, message, JobCounters.Zero);
}

internal sealed class JobRunner(
    IJobRegistry registry,
    IJobExecutionStore store,
    IJobLockProvider locks,
    IServiceScopeFactory scopes,
    IOptionsMonitor<BatchJobOptions> jobOptions,
    IOptions<BatchOptions> options,
    BatchMetrics metrics,
    TimeProvider time,
    ILoggerFactory loggerFactory,
    ILogger<JobRunner> logger) : IJobRunner
{
    private const int MaxMessageLength = 4000;

    public async Task<RunOutcome> RunAsync(RunRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        // 1. Résolution et validation : échecs rapides, en millisecondes, avant toute ressource.
        if (!registry.TryGet(request.JobName, out var descriptor))
        {
            RunnerLog.UnknownJob(logger, request.JobName);
            return RunOutcome.Rejected(ExitCodes.InvalidUsage, $"Unknown job '{request.JobName}'.");
        }

        var binding = descriptor.Bind(request.Parameters);
        if (!binding.IsValid)
        {
            var reason = string.Join("; ", binding.Errors);
            RunnerLog.InvalidParameters(logger, descriptor.Name, reason);
            return RunOutcome.Rejected(ExitCodes.InvalidUsage, $"Invalid parameters for '{descriptor.Name}': {reason}");
        }

        var settings = jobOptions.Get(descriptor.Name);
        var normalized = ParameterFingerprint.Normalize(request.Parameters);
        var parametersHash = ParameterFingerprint.Hash(normalized);

        // 2. Concurrence : un verrou distribué, pas une convention d'ordonnancement.
        var lockKey = settings.Concurrency switch
        {
            ConcurrencyMode.SingleInstance => $"batch:{descriptor.Name}",
            ConcurrencyMode.SingleInstancePerParameters => $"batch:{descriptor.Name}:{parametersHash}",
            _ => null,
        };

        await using var jobLock = lockKey is null ? null : await locks.TryAcquireAsync(lockKey, cancellationToken);
        if (lockKey is not null && jobLock is null)
        {
            RunnerLog.AlreadyRunning(logger, descriptor.Name, lockKey);
            metrics.RunRejected(descriptor.Name, "AlreadyRunning");
            return RunOutcome.Rejected(ExitCodes.AlreadyRunning, $"Job '{descriptor.Name}' is already running ({lockKey}).");
        }

        var now = time.GetUtcNow();

        // 3. Zombies : verrou libre + exécution « Running » = processus mort sans finaliser.
        if (lockKey is not null)
        {
            var scope = settings.Concurrency == ConcurrencyMode.SingleInstance ? null : parametersHash;
            var abandoned = await store.MarkAbandonedAsync(descriptor.Name, scope, now, cancellationToken);
            if (abandoned > 0)
            {
                RunnerLog.Abandoned(logger, descriptor.Name, abandoned);
            }
        }

        // 4. Reprise.
        JobExecutionRecord? resumeFrom = null;
        if (request.Restart)
        {
            var latest = await store.FindLatestAsync(descriptor.Name, parametersHash, cancellationToken);
            if (latest is not null && latest.Status.IsSuccessful())
            {
                RunnerLog.NothingToRestart(logger, descriptor.Name, latest.RunId);
                return new RunOutcome(ExitCodes.Success, latest.Status, latest.RunId, "Last execution already succeeded; nothing to restart.", latest.Counters);
            }

            if (latest is not null && latest.Status.IsTerminal())
            {
                resumeFrom = latest;
            }
        }

        // 5. Journal : l'exécution existe avant de commencer.
        var runId = Guid.CreateVersion7(now);
        await store.CreateAsync(
            new JobExecutionRecord
            {
                RunId = runId,
                JobName = descriptor.Name,
                ParametersHash = parametersHash,
                ParametersJson = ParameterFingerprint.ToJson(normalized),
                Trigger = request.Trigger,
                HostName = Environment.MachineName,
                StartedAt = now,
                LastHeartbeatAt = now,
                RestartOf = resumeFrom?.RunId,
                Checkpoint = resumeFrom?.Checkpoint,
            },
            cancellationToken);

        return await ExecuteAsync(descriptor, binding.Value!, settings, runId, resumeFrom, request.Trigger, jobLock, cancellationToken);
    }

    private async Task<RunOutcome> ExecuteAsync(
        JobDescriptor descriptor,
        object parameters,
        BatchJobOptions settings,
        Guid runId,
        JobExecutionRecord? resumeFrom,
        JobTrigger trigger,
        IJobLock? jobLock,
        CancellationToken hostToken)
    {
        var progress = new JobProgress();
        var jobLogger = loggerFactory.CreateLogger($"Batch.Jobs.{descriptor.Name}");

        using var logScope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["batch.job"] = descriptor.Name,
            ["batch.run_id"] = runId,
        });

        using var activity = BatchTelemetry.ActivitySource.StartActivity($"batch.run {descriptor.Name}");
        activity?.SetTag("batch.job", descriptor.Name);
        activity?.SetTag("batch.run_id", runId.ToString());
        activity?.SetTag("batch.trigger", trigger.ToString());
        activity?.SetTag("batch.restart_of", resumeFrom?.RunId.ToString());

        // 6. Annulation : arrêt de l'hôte, timeout du job, ou perte du verrou distribué.
        using var timeoutCts = settings.Timeout is { } timeout
            ? new CancellationTokenSource(timeout, time)
            : new CancellationTokenSource();
        var lostToken = jobLock?.LostToken ?? CancellationToken.None;
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(hostToken, timeoutCts.Token, lostToken);

        using var heartbeatCts = new CancellationTokenSource();
        var heartbeat = HeartbeatLoopAsync(runId, progress, heartbeatCts.Token);

        RunnerLog.Started(logger, descriptor.Name, runId, resumeFrom?.RunId, resumeFrom?.Checkpoint);
        var startedAt = time.GetTimestamp();

        JobStatus status;
        string message;
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var invocation = new RunInvocation
            {
                RunId = runId,
                JobName = descriptor.Name,
                Parameters = parameters,
                Trigger = trigger,
                RestartOf = resumeFrom?.RunId,
                ResumeCheckpoint = resumeFrom?.Checkpoint,
                Options = settings,
                Progress = progress,
                Logger = jobLogger,
                SaveCheckpointAsync = (checkpoint, ct) =>
                    store.SaveCheckpointAsync(runId, checkpoint, progress.Snapshot(), time.GetUtcNow(), ct),
            };

            var result = await descriptor.Execute(scope.ServiceProvider, invocation, runCts.Token);
            status = result.Status.IsTerminal() ? result.Status : JobStatus.Succeeded;
            message = result.Summary ?? status.ToString();

            // Des rejets, même tolérés, méritent un signal à l'exploitation.
            if (status == JobStatus.Succeeded && progress.Snapshot().Skipped > 0)
            {
                status = JobStatus.CompletedWithWarnings;
            }
        }
        catch (OperationCanceledException) when (hostToken.IsCancellationRequested)
        {
            status = JobStatus.Cancelled;
            message = "Cancelled by host (SIGTERM/SIGINT or scheduler shutdown). Restart with --restart.";
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            status = JobStatus.TimedOut;
            message = $"Timed out after {settings.Timeout}. Restart with --restart.";
        }
        catch (OperationCanceledException) when (lostToken.IsCancellationRequested)
        {
            status = JobStatus.TechnicalError;
            message = "Distributed lock lost (connection holding it dropped). Execution stopped to prevent a double run.";
        }
        catch (BatchFunctionalException ex)
        {
            status = JobStatus.FunctionalError;
            message = ex.Message;
            RunnerLog.FunctionalError(logger, ex, descriptor.Name);
        }
        catch (Exception ex)
        {
            status = JobStatus.TechnicalError;
            message = $"{ex.GetType().Name}: {ex.Message}";
            RunnerLog.TechnicalError(logger, ex, descriptor.Name);
        }
        finally
        {
            await heartbeatCts.CancelAsync();
            await heartbeat;
        }

        var counters = progress.Snapshot();
        var exitCode = ExitCodes.From(status);
        var elapsed = time.GetElapsedTime(startedAt);

        // 7. Finalisation : jamais avec le jeton annulé, sinon le statut final ne serait jamais écrit.
        using (var finalizeCts = new CancellationTokenSource(options.Value.FinalizationTimeout, time))
        {
            try
            {
                await store.CompleteAsync(runId, status, exitCode, Truncate(message), counters, time.GetUtcNow(), finalizeCts.Token);
            }
            catch (Exception ex)
            {
                // Le journal est indisponible : on rend quand même le vrai code retour à l'ordonnanceur.
                RunnerLog.FinalizationFailed(logger, ex, runId);
            }
        }

        metrics.RunCompleted(descriptor.Name, status, elapsed);
        activity?.SetTag("batch.status", status.ToString());
        activity?.SetTag("batch.items.read", counters.Read);
        activity?.SetTag("batch.items.written", counters.Written);
        activity?.SetTag("batch.items.skipped", counters.Skipped);
        activity?.SetStatus(status.IsSuccessful() ? ActivityStatusCode.Ok : ActivityStatusCode.Error, status.IsSuccessful() ? null : message);

        RunnerLog.Completed(logger, descriptor.Name, runId, status, exitCode, elapsed.TotalSeconds, counters.Read, counters.Written, counters.Skipped, message);
        return new RunOutcome(exitCode, status, runId, message, counters);
    }

    private async Task HeartbeatLoopAsync(Guid runId, JobProgress progress, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(options.Value.HeartbeatInterval, time);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await store.HeartbeatAsync(runId, progress.Snapshot(), time.GetUtcNow(), cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Un heartbeat manqué n'arrête pas le job : seul le verrou fait foi pour la concurrence.
                    RunnerLog.HeartbeatFailed(logger, ex, runId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Arrêt normal de la boucle.
        }
    }

    private static string Truncate(string value) =>
        value.Length <= MaxMessageLength ? value : value[..MaxMessageLength];
}

internal static partial class RunnerLog
{
    [LoggerMessage(1000, LogLevel.Error, "Unknown job '{JobName}'. Use 'list' to see registered jobs.")]
    public static partial void UnknownJob(ILogger logger, string jobName);

    [LoggerMessage(1001, LogLevel.Error, "Invalid parameters for job '{JobName}': {Reason}")]
    public static partial void InvalidParameters(ILogger logger, string jobName, string reason);

    [LoggerMessage(1002, LogLevel.Warning, "Job '{JobName}' is already running (lock {LockKey}). Skipping this execution.")]
    public static partial void AlreadyRunning(ILogger logger, string jobName, string lockKey);

    [LoggerMessage(1003, LogLevel.Warning, "Marked {Count} stale execution(s) of '{JobName}' as Abandoned.")]
    public static partial void Abandoned(ILogger logger, string jobName, int count);

    [LoggerMessage(1004, LogLevel.Information, "Last execution {RunId} of '{JobName}' already succeeded; nothing to restart.")]
    public static partial void NothingToRestart(ILogger logger, string jobName, Guid runId);

    [LoggerMessage(1005, LogLevel.Information, "Starting '{JobName}' run {RunId} (restart of {RestartOf}, from checkpoint {Checkpoint}).")]
    public static partial void Started(ILogger logger, string jobName, Guid runId, Guid? restartOf, string? checkpoint);

    [LoggerMessage(1006, LogLevel.Information,
        "Finished '{JobName}' run {RunId}: {Status} (exit {ExitCode}) in {ElapsedSeconds:F1}s. Read={Read} Written={Written} Skipped={Skipped}. {Message}")]
    public static partial void Completed(
        ILogger logger, string jobName, Guid runId, JobStatus status, int exitCode, double elapsedSeconds, long read, long written, long skipped, string message);

    [LoggerMessage(1007, LogLevel.Error, "Functional error in job '{JobName}'.")]
    public static partial void FunctionalError(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1008, LogLevel.Error, "Technical error in job '{JobName}'.")]
    public static partial void TechnicalError(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1009, LogLevel.Warning, "Heartbeat failed for run {RunId}.")]
    public static partial void HeartbeatFailed(ILogger logger, Exception exception, Guid runId);

    [LoggerMessage(1010, LogLevel.Error, "Could not write the final status of run {RunId} to the journal.")]
    public static partial void FinalizationFailed(ILogger logger, Exception exception, Guid runId);
}
