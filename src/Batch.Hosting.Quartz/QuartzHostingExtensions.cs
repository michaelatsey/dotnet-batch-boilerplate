using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Quartz;

namespace Batch.Hosting.Quartz;

/// <summary>Section <c>Batch:Quartz</c>.</summary>
public sealed class BatchQuartzOptions
{
    public const string SectionName = "Batch:Quartz";

    /// <summary><c>Memory</c> (un seul nœud) ou <c>Postgres</c> (cluster, reprise des déclenchements manqués).</summary>
    public string Store { get; set; } = "Memory";

    /// <summary>Nom de la chaîne de connexion utilisée par le JobStore persistant.</summary>
    public string ConnectionStringName { get; set; } = "Batch";

    /// <summary>Crée les tables Quartz si absentes. Développement uniquement : en production, scripts du DBA.</summary>
    public bool ProvisionSchema { get; set; }

    public string TablePrefix { get; set; } = "qrtz_";

    public int MaxConcurrency { get; set; } = 4;
}

/// <summary>
/// Une planification : un job batch, une expression cron, des paramètres.
/// Section <c>Batch:Schedules:&lt;nom&gt;</c> : un dictionnaire, pour que chaque environnement
/// surcharge ou ajoute une planification par son nom (un tableau fusionnerait par index).
/// </summary>
public sealed class BatchSchedule
{
    public const string SectionName = "Batch:Schedules";

    /// <summary>Identité Quartz stable, issue de la clé de configuration.</summary>
    public string Name { get; set; } = "";

    public string Job { get; set; } = "";

    /// <summary>Cron Quartz à 6 ou 7 champs (secondes en tête), ou cron Unix à 5 champs.</summary>
    public string Cron { get; set; } = "";

    /// <summary>Identifiant IANA, par exemple <c>Europe/Paris</c>. Défaut : UTC.</summary>
    public string? TimeZone { get; set; }

    public string? Description { get; set; }

    public Dictionary<string, string?> Parameters { get; set; } = [];
}

public static class QuartzHostingExtensions
{
    /// <summary>
    /// Ajoute Quartz et transforme chaque entrée de <c>Batch:Schedules</c> en job + trigger.
    /// Les planifications sont déclaratives : modifier un horaire ne demande aucune recompilation.
    /// </summary>
    public static IHostApplicationBuilder AddBatchQuartzScheduler(this IHostApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = builder.Configuration.GetSection(BatchQuartzOptions.SectionName).Get<BatchQuartzOptions>() ?? new();
        var schedules = ReadSchedules(builder.Configuration);
        Validate(schedules);

        builder.AddQuartz(q =>
        {
            q.UseDefaultThreadPool(options.MaxConcurrency);

            if (string.Equals(options.Store, "Postgres", StringComparison.OrdinalIgnoreCase))
            {
                var connectionString = builder.Configuration.GetConnectionString(options.ConnectionStringName)
                    ?? throw new InvalidOperationException(
                        $"Batch:Quartz:Store is 'Postgres' but ConnectionStrings:{options.ConnectionStringName} is missing.");

                q.UsePersistentStore(store =>
                {
                    store.UsePostgres(connectionString);
                    store.ConfigureStore(o => o.TablePrefix = options.TablePrefix);
                    store.UseClustering(cluster =>
                    {
                        cluster.CheckinInterval = TimeSpan.FromSeconds(10);
                        cluster.CheckinMisfireThreshold = TimeSpan.FromSeconds(20);
                    });

                    if (options.ProvisionSchema)
                    {
                        store.ProvisionSchema();
                    }
                });
            }

            foreach (var schedule in schedules)
            {
                var jobKey = new JobKey(schedule.Name, "batch");
                var timeZone = schedule.TimeZone is null
                    ? TimeZoneInfo.Utc
                    : TimeZoneInfo.FindSystemTimeZoneById(schedule.TimeZone);

                q.AddJob<QuartzBatchJob>(job =>
                {
                    job.WithIdentity(jobKey)
                        .WithDescription(schedule.Description ?? $"Batch job '{schedule.Job}'")
                        .StoreDurably()
                        .UsingJobData(QuartzBatchJob.JobNameKey, schedule.Job);

                    foreach (var (key, value) in schedule.Parameters)
                    {
                        job.UsingJobData(QuartzBatchJob.ParameterPrefix + key, value);
                    }
                });

                q.AddTrigger<QuartzBatchJob>(trigger => trigger
                    .ForJob(jobKey)
                    .WithIdentity($"{schedule.Name}-trigger", "batch")
                    .WithCronSchedule(schedule.Cron, cron => cron.InTimeZone(timeZone)));
            }
        });

        builder.AddQuartzHostedService(o => o.WaitForJobsToComplete = true);
        return builder;
    }

    /// <summary>Lit <c>Batch:Schedules</c> ; le nom de chaque planification est sa clé.</summary>
    public static List<BatchSchedule> ReadSchedules(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var section = configuration.GetSection(BatchSchedule.SectionName).Get<Dictionary<string, BatchSchedule>>() ?? [];
        return section
            .Select(entry =>
            {
                entry.Value.Name = entry.Key;
                return entry.Value;
            })
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ToList();
    }

    private static void Validate(List<BatchSchedule> schedules)
    {
        var errors = new List<string>();
        foreach (var schedule in schedules)
        {
            if (string.IsNullOrWhiteSpace(schedule.Job))
            {
                errors.Add($"{BatchSchedule.SectionName}:{schedule.Name}:Job is required.");
            }

            if (string.IsNullOrWhiteSpace(schedule.Cron))
            {
                errors.Add($"{BatchSchedule.SectionName}:{schedule.Name}:Cron is required.");
            }
        }

        if (errors.Count > 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, errors));
        }
    }
}
