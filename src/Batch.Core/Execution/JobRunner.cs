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

        // Un job que le conteneur DI ne sait pas construire (chaîne de connexion absente...) est une erreur
        // de configuration : relancer ne changera rien.
        if (await CannotConstructAsync(descriptor) is { } constructionError)
        {
            RunnerLog.CannotConstruct(logger, constructionError, descriptor.Name);
            return RunOutcome.Rejected(
                ExitCodes.ConfigurationError,
                $"Job '{descriptor.Name}' cannot be created (missing configuration or dependency): {constructionError.Message}");
        }

        var settings = jobOptions.Get(descriptor.Name);
        var (parametersJson, parametersHash) = ParameterFingerprint.Of(binding.Value!, descriptor.ParametersType);

        // 2. Concurrence : un verrou distribué, pas une convention d'ordonnancement.
        var lockKey = settings.Concurrency switch
        {
            ConcurrencyMode.SingleInstance => $"batch:{descriptor.Name}",
            ConcurrencyMode.SingleInstancePerParameters => $"batch:{descriptor.Name}:{parametersHash}",
            _ => null,
        };

        // Démarrage (verrou, zombies, reprise, journal) : tout passe par l'infrastructure. Une panne ici
        // doit rester une erreur technique (3), jamais une exception brute que l'hôte traduirait en 1.
        IJobLock? jobLock = null;
        try
        {
            Start start;
            try
            {
                if (lockKey is not null)
                {
                    jobLock = await locks.TryAcquireAsync(lockKey, cancellationToken);
                    if (jobLock is null)
                    {
                        RunnerLog.AlreadyRunning(logger, descriptor.Name, lockKey);
                        metrics.RunRejected(descriptor.Name, "AlreadyRunning");
                        return RunOutcome.Rejected(ExitCodes.AlreadyRunning, $"Job '{descriptor.Name}' is already running ({lockKey}).");
                    }
                }

                start = await StartAsync(descriptor, binding.Value!, settings, request, lockKey, parametersHash, parametersJson, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return RunOutcome.Rejected(ExitCodes.Cancelled, "Cancelled before start.");
            }
            catch (Exception ex)
            {
                RunnerLog.StartFailed(logger, ex, descriptor.Name);
                metrics.RunRejected(descriptor.Name, "StartFailed");
                return RunOutcome.Rejected(
                    ExitCodes.TechnicalError,
                    $"Could not start '{descriptor.Name}': {ex.GetType().Name}: {ex.Message}");
            }

            return start.Early
                ?? await ExecuteAsync(descriptor, binding.Value!, settings, start.RunId, start.ResumeFrom, request.Trigger, jobLock, cancellationToken);
        }
        finally
        {
            await ReleaseAsync(jobLock, descriptor.Name);
        }
    }

    /// <summary>Zombies, identité de l'entrée, reprise, journal. Appelé verrou détenu.</summary>
    private async Task<Start> StartAsync(
        JobDescriptor descriptor,
        object parameters,
        BatchJobOptions settings,
        RunRequest request,
        string? lockKey,
        string parametersHash,
        string parametersJson,
        CancellationToken cancellationToken)
    {
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

        // 4. Identité de la donnée d'entrée : un checkpoint n'a de sens que sur la donnée qui l'a produit.
        var input = await ResolveInputIdentityAsync(descriptor, parameters, cancellationToken);

        // 5. Reprise.
        JobExecutionRecord? resumeFrom = null;
        if (request.Restart)
        {
            var latest = await store.FindLatestAsync(descriptor.Name, parametersHash, cancellationToken);
            if (latest is not null && !input.Matches(latest.InputIdentity))
            {
                // Mêmes paramètres, autre donnée (nouveau fichier au même chemin) : ni reprise, ni « déjà fait ».
                RunnerLog.InputChanged(logger, descriptor.Name, latest.RunId, latest.Status);
                latest = null;
            }

            if (latest is not null && latest.Status.IsSuccessful())
            {
                RunnerLog.NothingToRestart(logger, descriptor.Name, latest.RunId);
                return new Start(
                    new RunOutcome(ExitCodes.Success, latest.Status, latest.RunId, "Last execution already succeeded; nothing to restart.", latest.Counters),
                    latest.RunId,
                    null);
            }

            if (latest is not null && latest.Status.IsTerminal())
            {
                resumeFrom = latest;
            }
        }

        // 6. Journal : l'exécution existe avant de commencer.
        var runId = Guid.CreateVersion7(now);
        await store.CreateAsync(
            new JobExecutionRecord
            {
                RunId = runId,
                JobName = descriptor.Name,
                ParametersHash = parametersHash,
                ParametersJson = parametersJson,
                InputIdentity = input.Value,
                Trigger = request.Trigger,
                HostName = Environment.MachineName,
                StartedAt = now,
                LastHeartbeatAt = now,
                RestartOf = resumeFrom?.RunId,
                Checkpoint = resumeFrom?.Checkpoint,
            },
            cancellationToken);

        return new Start(null, runId, resumeFrom);
    }

    /// <summary>Issue du démarrage : soit une réponse immédiate (<see cref="Early"/>), soit un run journalisé.</summary>
    private readonly record struct Start(RunOutcome? Early, Guid RunId, JobExecutionRecord? ResumeFrom);

    private async Task<Exception?> CannotConstructAsync(JobDescriptor descriptor)
    {
        try
        {
            // Scope asynchrone : un job peut n'implémenter que IAsyncDisposable.
            await using var scope = scopes.CreateAsyncScope();
            _ = descriptor.CreateJob(scope.ServiceProvider);
            return null;
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            return ex;
        }
    }

    private async Task ReleaseAsync(IJobLock? jobLock, string jobName)
    {
        if (jobLock is null)
        {
            return;
        }

        try
        {
            await jobLock.DisposeAsync();
        }
        catch (Exception ex)
        {
            // Le statut est déjà journalisé ; la base libère un verrou advisory à la fermeture de sa connexion.
            RunnerLog.ReleaseFailed(logger, ex, jobName);
        }
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

        // 7. Annulation : arrêt de l'hôte, timeout du job, ou perte du verrou distribué.
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

        // 8. Finalisation : jamais avec le jeton annulé, sinon le statut final ne serait jamais écrit.
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

    private async Task<InputIdentity> ResolveInputIdentityAsync(JobDescriptor descriptor, object parameters, CancellationToken cancellationToken)
    {
        if (descriptor.ComputeInputIdentity is null)
        {
            return InputIdentity.NotDeclared;
        }

        try
        {
            await using var scope = scopes.CreateAsyncScope();
            return InputIdentity.Known(await descriptor.ComputeInputIdentity(scope.ServiceProvider, parameters, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // Sans preuve que la donnée est la même, on ne reprend pas : l'erreur réelle remontera de la lecture.
            RunnerLog.InputIdentityFailed(logger, ex, descriptor.Name);
            return InputIdentity.Unknown;
        }
    }

    /// <summary>Résultat du calcul d'identité : non déclarée, connue, ou impossible à établir.</summary>
    private readonly record struct InputIdentity(string? Value, bool IsUnknown)
    {
        public static InputIdentity NotDeclared => default;

        public static InputIdentity Unknown => new(null, IsUnknown: true);

        public static InputIdentity Known(string value) => new(value, IsUnknown: false);

        /// <summary>Une identité inconnue ne correspond à rien, pas même à une autre identité inconnue.</summary>
        public bool Matches(string? previous) => !IsUnknown && string.Equals(Value, previous, StringComparison.Ordinal);
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

    [LoggerMessage(1013, LogLevel.Error, "Job '{JobName}' cannot be created by the DI container (configuration error).")]
    public static partial void CannotConstruct(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1014, LogLevel.Error, "Could not start job '{JobName}' (lock or journal unavailable).")]
    public static partial void StartFailed(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1015, LogLevel.Warning, "Could not release the lock of job '{JobName}'.")]
    public static partial void ReleaseFailed(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1011, LogLevel.Warning, "Could not compute the input identity of '{JobName}'; a restart will start fresh instead of resuming.")]
    public static partial void InputIdentityFailed(ILogger logger, Exception exception, string jobName);

    [LoggerMessage(1012, LogLevel.Warning,
        "Input of '{JobName}' changed since run {RunId} ({Status}): starting fresh instead of resuming or skipping.")]
    public static partial void InputChanged(ILogger logger, string jobName, Guid runId, JobStatus status);
}
