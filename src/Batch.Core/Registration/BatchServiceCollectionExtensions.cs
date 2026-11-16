using System.Text.RegularExpressions;
using Batch.Abstractions;
using Batch.Core.Chunking;
using Batch.Core.Execution;
using Batch.Core.Locking;
using Batch.Core.Persistence;
using Batch.Core.Resilience;
using Batch.Core.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Batch.Core.Registration;

public static class BatchServiceCollectionExtensions
{
    /// <summary>
    /// Enregistre le moteur avec des implémentations en mémoire (journal, verrou).
    /// Un module d'infrastructure (PostgreSQL...) les remplace ensuite.
    /// </summary>
    public static BatchBuilder AddBatch(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(d => d.ServiceType == typeof(BatchMarker)))
        {
            return new BatchBuilder(services);
        }

        services.AddSingleton<BatchMarker>();

        services.AddOptions<BatchOptions>()
            .BindConfiguration(BatchOptions.SectionName)
            .Validate(o => o.HeartbeatInterval > TimeSpan.Zero, "Batch:HeartbeatInterval must be positive.")
            .Validate(o => o.FinalizationTimeout > TimeSpan.Zero, "Batch:FinalizationTimeout must be positive.")
            .Validate(o => o.Retry.MaxAttempts >= 0, "Batch:Retry:MaxAttempts must be >= 0.")
            .ValidateOnStart();

        services.AddMetrics();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<BatchMetrics>();
        services.TryAddSingleton<IJobRegistry, JobRegistry>();
        services.TryAddSingleton<IJobRunner, JobRunner>();
        services.TryAddSingleton<ChunkEngine>();
        services.TryAddSingleton<ITransientErrorClassifier, DefaultTransientErrorClassifier>();
        services.TryAddSingleton<IJobExecutionStore, InMemoryJobExecutionStore>();
        services.TryAddSingleton<IJobLockProvider, InMemoryJobLockProvider>();

        services.AddResiliencePipeline(BatchResilience.PipelineName, static (builder, context) =>
        {
            var retry = context.ServiceProvider.GetRequiredService<IOptions<BatchOptions>>().Value.Retry;
            var classifier = context.ServiceProvider.GetRequiredService<ITransientErrorClassifier>();

            if (retry.MaxAttempts > 0)
            {
                builder.AddRetry(new RetryStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder().Handle<Exception>(classifier.IsTransient),
                    MaxRetryAttempts = retry.MaxAttempts,
                    Delay = retry.BaseDelay,
                    MaxDelay = retry.MaxDelay,
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,
                });
            }
        });

        return new BatchBuilder(services);
    }

    private sealed class BatchMarker;
}

/// <summary>Point d'entrée fluide pour déclarer les jobs.</summary>
public sealed partial class BatchBuilder
{
    internal BatchBuilder(IServiceCollection services) => Services = services;

    public IServiceCollection Services { get; }

    /// <summary>Déclare un job « tâche » exécuté d'un bloc.</summary>
    public BatchBuilder AddJob<TJob, TParams>(string name, Action<BatchJobOptions>? configure = null)
        where TJob : class, IBatchJob<TParams>
        where TParams : class, new()
    {
        RegisterCommon<TJob>(name, configure);
        Services.AddSingleton(JobDescriptor.ForTask<TJob, TParams>(name));
        return this;
    }

    /// <summary>Déclare un job par chunks : lecture en flux, traitement parallèle borné, écriture ordonnée.</summary>
    public BatchBuilder AddChunkedJob<TJob, TParams, TIn, TOut>(string name, Action<BatchJobOptions>? configure = null)
        where TJob : class, IChunkedJob<TParams, TIn, TOut>
        where TParams : class, new()
        where TIn : notnull
        where TOut : class
    {
        RegisterCommon<TJob>(name, configure);
        Services.AddSingleton(JobDescriptor.ForChunked<TJob, TParams, TIn, TOut>(name));
        return this;
    }

    private void RegisterCommon<TJob>(string name, Action<BatchJobOptions>? configure)
        where TJob : class
    {
        if (!JobNamePattern().IsMatch(name))
        {
            throw new ArgumentException(
                $"Invalid job name '{name}'. Use lowercase kebab-case, e.g. 'import-invoices'.",
                nameof(name));
        }

        // Un scope DI par exécution : DbContext et autres dépendances scoped vivent le temps du run.
        Services.TryAddScoped<TJob>();

        var options = Services.AddOptions<BatchJobOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        // La configuration passe après le code : l'exploitation garde le dernier mot.
        options
            .BindConfiguration($"{BatchOptions.SectionName}:Jobs:{name}")
            .Validate(o => o.ChunkSize > 0, $"Batch:Jobs:{name}:ChunkSize must be > 0.")
            .Validate(o => o.MaxDegreeOfParallelism > 0, $"Batch:Jobs:{name}:MaxDegreeOfParallelism must be > 0.")
            .Validate(o => o.PipelineDepth > 0, $"Batch:Jobs:{name}:PipelineDepth must be > 0.")
            .Validate(o => o.SkipLimit >= 0, $"Batch:Jobs:{name}:SkipLimit must be >= 0.")
            .Validate(o => o.Timeout is null || o.Timeout > TimeSpan.Zero, $"Batch:Jobs:{name}:Timeout must be positive.")
            .ValidateOnStart();
    }

    [GeneratedRegex("^[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions.CultureInvariant)]
    private static partial Regex JobNamePattern();
}
