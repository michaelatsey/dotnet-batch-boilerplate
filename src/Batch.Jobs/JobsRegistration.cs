using Batch.Core;
using Batch.Core.Registration;
using Batch.Infrastructure.PostgreSql;
using Batch.Jobs.ImportInvoices;
using Batch.Jobs.PurgeExecutions;
using Microsoft.Extensions.DependencyInjection;

namespace Batch.Jobs;

/// <summary>
/// Catalogue des jobs. Ajouter un job = créer son dossier et ajouter une ligne ici.
/// Les valeurs ci-dessous sont des défauts : l'exploitation les surcharge dans <c>Batch:Jobs:&lt;nom&gt;</c>.
/// </summary>
public static class JobsRegistration
{
    public static BatchBuilder AddBatchJobs(this BatchBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.AddJob<PurgeExecutionsJob, PurgeExecutionsParameters>("purge-executions", o =>
        {
            o.Description = "Deletes journal entries older than RetentionDays.";
            o.Concurrency = ConcurrencyMode.SingleInstance;
            o.Timeout = TimeSpan.FromMinutes(30);
        });

        builder.AddChunkedJob<ImportInvoicesJob, ImportInvoicesParameters, InvoiceLine, Invoice>("import-invoices", o =>
        {
            o.Description = "Imports a CSV file of invoices into billing.invoice (idempotent upsert).";
            o.ChunkSize = 1_000;
            o.MaxDegreeOfParallelism = Environment.ProcessorCount;
            o.SkipLimit = 100;
            o.Timeout = TimeSpan.FromHours(2);
        });

        builder.Services.AddSingleton(
            SchemaScript.FromEmbeddedResource(typeof(ImportInvoicesJob).Assembly, "billing-invoice.sql", order: 100));

        return builder;
    }
}
