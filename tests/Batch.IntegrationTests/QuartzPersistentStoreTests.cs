using Batch.Core.Registration;
using Batch.Hosting.Quartz;
using Batch.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Batch.IntegrationTests;

/// <summary>JobStore Quartz clusterisé sur PostgreSQL : la configuration reste la source de vérité.</summary>
[Collection(PostgresCollection.Name)]
public sealed class QuartzPersistentStoreTests(PostgresFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Schedule_removed_from_configuration_is_removed_from_the_store()
    {
        // Déploiement N : deux planifications, persistées dans les tables qrtz_.
        using (var before = BuildHost("nightly", "hourly"))
        {
            await before.StartAsync(Ct);
            Assert.Equal(["hourly", "nightly"], await StoredSchedulesAsync(before));
            await before.StopAsync(Ct);
        }

        // Déploiement N+1 : « hourly » a été retirée d'appsettings.
        var logs = new CapturedLogs();
        using var after = BuildHost(logs, "nightly");
        await after.StartAsync(Ct);

        Assert.Equal(["nightly"], await StoredSchedulesAsync(after));
        await after.StopAsync(Ct);

        // Action destructive : une trace Warning avec le nom et le cron de ce qui a été supprimé.
        var removal = Assert.Single(logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("Removed Quartz schedule", StringComparison.Ordinal));
        Assert.Contains("'hourly'", removal.Message, StringComparison.Ordinal);
        Assert.Contains("0 0 3 1 1 ?", removal.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(logs.Entries, e => e.Message.Contains("'nightly'", StringComparison.Ordinal));
    }

    private IHost BuildHost(params string[] schedules) => BuildHost(logs: null, schedules);

    private IHost BuildHost(CapturedLogs? logs, params string[] schedules)
    {
        var settings = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Batch"] = postgres.ConnectionString,
            ["Batch:Quartz:Store"] = "Postgres",
            ["Batch:Quartz:ProvisionSchema"] = "true",
        };
        foreach (var name in schedules)
        {
            settings[$"Batch:Schedules:{name}:Job"] = "purge-executions";
            settings[$"Batch:Schedules:{name}:Cron"] = "0 0 3 1 1 ?"; // 1er janvier : ne se déclenche pas pendant le test
        }

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        if (logs is not null)
        {
            builder.Logging.AddProvider(logs);
        }

        builder.Services.AddBatch().AddBatchJobs();
        builder.AddBatchQuartzScheduler();
        return builder.Build();
    }

    private static async Task<List<string>> StoredSchedulesAsync(IHost host)
    {
        var scheduler = await host.Services.GetRequiredService<ISchedulerFactory>().GetScheduler(Ct);
        var keys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(QuartzHostingExtensions.JobGroup), Ct);
        return keys.Select(k => k.Name).Order(StringComparer.Ordinal).ToList();
    }
}

/// <summary>Fournisseur de logs en mémoire : vérifie qu'une action laisse bien sa trace.</summary>
internal sealed class CapturedLogs : ILoggerProvider
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (_entries)
            {
                return [.. _entries];
            }
        }
    }

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(CapturedLogs owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (owner._entries)
            {
                owner._entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
