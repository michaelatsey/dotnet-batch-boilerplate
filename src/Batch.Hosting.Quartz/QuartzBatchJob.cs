using Batch.Abstractions;
using Batch.Core.Execution;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Batch.Hosting.Quartz;

/// <summary>
/// Pont unique entre Quartz et le moteur. Quartz ne connaît que ce type ;
/// le job batch réel et ses paramètres voyagent dans le JobDataMap.
/// </summary>
/// <remarks>
/// <see cref="DisallowConcurrentExecutionAttribute"/> évite deux exécutions d'une même planification
/// sur ce nœud ; le verrou distribué du moteur couvre le cluster et les lancements CLI concurrents.
/// </remarks>
[DisallowConcurrentExecution]
public sealed partial class QuartzBatchJob(IJobRunner runner, ILogger<QuartzBatchJob> logger) : IJob
{
    public const string JobNameKey = "batch.job";
    public const string ParameterPrefix = "param.";

    public async ValueTask Execute(IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        string? jobName = null;
        var parameters = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in context.MergedJobDataMap)
        {
            if (entry.Key == JobNameKey)
            {
                jobName = entry.Value?.ToString();
            }
            else if (entry.Key.StartsWith(ParameterPrefix, StringComparison.Ordinal))
            {
                parameters[entry.Key[ParameterPrefix.Length..]] = entry.Value?.ToString();
            }
        }

        if (string.IsNullOrWhiteSpace(jobName))
        {
            throw new JobExecutionException($"Quartz job {context.JobDetail.Key} has no '{JobNameKey}' entry in its JobDataMap.");
        }

        var outcome = await runner.RunAsync(new RunRequest(jobName, parameters, JobTrigger.Quartz), cancellationToken);
        LogOutcome(logger, context.JobDetail.Key.ToString(), jobName, outcome.ExitCode, outcome.Message);

        switch (outcome.ExitCode)
        {
            case ExitCodes.Success:
            case ExitCodes.CompletedWithWarnings:
                context.Result = JobRunReport.Succeeded(outcome.Message)
                    .With("read", outcome.Counters.Read)
                    .With("written", outcome.Counters.Written)
                    .With("skipped", outcome.Counters.Skipped);
                break;

            case ExitCodes.AlreadyRunning:
                // Un autre nœud (ou un lancement CLI) fait déjà le travail : ce n'est pas un échec.
                context.Result = JobRunReport.Skipped(outcome.Message);
                break;

            case ExitCodes.TechnicalError:
            case ExitCodes.TimedOut:
                // Exception : déclenche la politique de retry Quartz si elle est configurée.
                throw new JobExecutionException(outcome.Message);

            default:
                // Fonctionnel, usage invalide, annulation : relancer ne servirait à rien.
                context.Result = JobRunReport.Failed(outcome.Message);
                break;
        }
    }

    [LoggerMessage(4000, LogLevel.Information, "Quartz {QuartzJob} ran batch job '{JobName}': exit {ExitCode}. {Message}")]
    private static partial void LogOutcome(ILogger logger, string quartzJob, string jobName, int exitCode, string message);
}
