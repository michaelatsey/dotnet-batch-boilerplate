using Batch.Core.Locking;
using Batch.Core.Persistence;
using Batch.Core.Registration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Batch.Infrastructure.PostgreSql;

public static class PostgresBatchBuilderExtensions
{
    /// <summary>
    /// Remplace le journal et le verrou en mémoire par PostgreSQL, et enregistre une
    /// <see cref="NpgsqlDataSource"/> partagée, utilisable par les jobs.
    /// </summary>
    public static BatchBuilder UsePostgres(this BatchBuilder builder, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var services = builder.Services;

        services.TryAddSingleton(_ => new NpgsqlDataSourceBuilder(connectionString).Build());
        services.AddPooledDbContextFactory<BatchDbContext>(
            (sp, options) => options.UseNpgsql(sp.GetRequiredService<NpgsqlDataSource>()));

        services.Replace(ServiceDescriptor.Singleton<IJobExecutionStore, EfJobExecutionStore>());
        services.Replace(ServiceDescriptor.Singleton<IJobLockProvider>(_ => new PostgresJobLockProvider(connectionString)));

        services.AddSingleton(SchemaScript.FromEmbeddedResource(typeof(BatchDbContext).Assembly, "batch-journal.sql", order: 0));
        services.TryAddSingleton<SchemaInitializer>();

        return builder;
    }
}
