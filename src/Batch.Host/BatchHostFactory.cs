using Batch.Core.Registration;
using Batch.Core.Telemetry;
using Batch.Hosting.Quartz;
using Batch.Infrastructure.PostgreSql;
using Batch.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Batch.Host;

internal enum HostMode
{
    /// <summary>Exécution unique (`run`) ou commande d'administration : aucun ordonnanceur interne.</summary>
    Command,

    /// <summary>Processus long (`serve`) : Quartz déclenche les jobs.</summary>
    Scheduler,
}

internal static class BatchHostFactory
{
    public const string ConnectionStringName = "Batch";

    public static IHost Build(HostMode mode, Action<IHostApplicationBuilder>? configure = null)
    {
        // Les arguments ne sont PAS transmis à la configuration : System.CommandLine les possède.
        // ContentRoot = dossier de l'exécutable : l'ordonnanceur peut lancer depuis n'importe quel répertoire.
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            ContentRootPath = AppContext.BaseDirectory,
        });

        builder.Configuration.AddEnvironmentVariables(prefix: "BATCH_");
        configure?.Invoke(builder);

        var batch = builder.Services.AddBatch().AddBatchJobs();

        var connectionString = builder.Configuration.GetConnectionString(ConnectionStringName);
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            batch.UsePostgres(connectionString);
        }

        AddTelemetry(builder);

        if (mode == HostMode.Scheduler)
        {
            builder.AddBatchQuartzScheduler();
        }

        return builder.Build();
    }

    private static void AddTelemetry(HostApplicationBuilder builder)
    {
        var serviceName = builder.Configuration["Batch:ServiceName"] ?? "batch-host";

        var otel = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing => tracing.AddSource(BatchTelemetry.SourceName, "Quartz"))
            .WithMetrics(metrics => metrics.AddMeter(BatchTelemetry.MeterName, "Quartz"))
            .WithLogging();

        // Export activé seulement si un collecteur est configuré (variable standard OpenTelemetry).
        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
        {
            otel.UseOtlpExporter();
        }
    }
}
