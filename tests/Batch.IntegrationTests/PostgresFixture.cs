using Batch.Core.Registration;
using Batch.Infrastructure.PostgreSql;
using Batch.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Batch.IntegrationTests;

/// <summary>Un PostgreSQL réel par session de tests : verrous advisory et COPY ne se simulent pas.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17").Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync(TestContext.Current.CancellationToken);
        await using var sp = BuildServices();
        await sp.GetRequiredService<SchemaInitializer>().ApplyAsync(CancellationToken.None);
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();

    public ServiceProvider BuildServices(Dictionary<string, string?>? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection(configuration ?? []).Build());
        services.AddLogging();
        services.AddBatch().AddBatchJobs().UsePostgres(ConnectionString);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    public async Task<long> ScalarAsync(string sql)
    {
        await using var dataSource = NpgsqlDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }
}

[CollectionDefinition(PostgresCollection.Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}
