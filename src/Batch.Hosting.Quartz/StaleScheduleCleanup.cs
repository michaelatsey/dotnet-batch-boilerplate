using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Batch.Hosting.Quartz;

/// <summary>Noms des planifications déclarées dans <c>Batch:Schedules</c>.</summary>
internal sealed record ConfiguredSchedules(IReadOnlySet<string> Names);

/// <summary>
/// Rend les planifications réellement déclaratives avec un JobStore persistant : Quartz ajoute ou remplace
/// les jobs déclarés, mais ne supprime jamais ceux qui ont disparu de la configuration. Sans ce nettoyage,
/// une planification retirée d'appsettings continue de se déclencher depuis les tables <c>qrtz_</c>.
/// </summary>
/// <remarks>
/// Le groupe <see cref="QuartzHostingExtensions.JobGroup"/> appartient à la configuration : tout job de ce groupe
/// absent de <c>Batch:Schedules</c> est supprimé au démarrage, avec ses triggers. Enregistré avant le service
/// hébergé de Quartz : le nettoyage a lieu avant que ce nœud ne déclenche quoi que ce soit.
/// </remarks>
internal sealed partial class StaleScheduleCleanup(
    ISchedulerFactory schedulerFactory,
    ConfiguredSchedules configured,
    ILogger<StaleScheduleCleanup> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var scheduler = await schedulerFactory.GetScheduler(cancellationToken);
        var keys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(QuartzHostingExtensions.JobGroup), cancellationToken);

        foreach (var key in keys.Where(k => !configured.Names.Contains(k.Name)))
        {
            // Lu avant la suppression : c'est la seule action destructive, la trace doit dire ce qui disparaît.
            var triggers = await scheduler.GetTriggersOfJob(key, cancellationToken);
            var cron = triggers.Count == 0 ? "(no trigger)" : string.Join(", ", triggers.Select(Describe));

            if (await scheduler.DeleteJob(key, cancellationToken))
            {
                LogRemoved(logger, key.Name, cron);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static string Describe(ITrigger trigger) => trigger is ICronTrigger cron
        ? $"{cron.CronExpressionString} {cron.TimeZone.Id}"
        : trigger.GetType().Name;

    [LoggerMessage(4100, LogLevel.Warning,
        "Removed Quartz schedule '{Schedule}' (cron {Cron}) from the job store: no longer declared in Batch:Schedules.")]
    private static partial void LogRemoved(ILogger logger, string schedule, string cron);
}
