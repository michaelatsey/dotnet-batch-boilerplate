using Batch.Abstractions;
using Batch.Core.Chunking;
using Batch.Core.Execution;
using Batch.Core.Parameters;
using Microsoft.Extensions.DependencyInjection;

namespace Batch.Core.Registration;

/// <summary>
/// Description d'un job enregistré. Les délégués sont construits à l'enregistrement, avec les types génériques
/// connus : aucune réflexion à l'exécution.
/// </summary>
public sealed class JobDescriptor
{
    private JobDescriptor(
        string name,
        Type jobType,
        Type parametersType,
        bool isChunked,
        Func<IReadOnlyDictionary<string, string?>, ParameterBindingResult> bind,
        Func<IServiceProvider, RunInvocation, CancellationToken, Task<JobResult>> execute,
        Func<IServiceProvider, object, CancellationToken, ValueTask<string>>? computeInputIdentity)
    {
        Name = name;
        JobType = jobType;
        ParametersType = parametersType;
        IsChunked = isChunked;
        Bind = bind;
        Execute = execute;
        ComputeInputIdentity = computeInputIdentity;
    }

    public string Name { get; }

    public Type JobType { get; }

    public Type ParametersType { get; }

    public bool IsChunked { get; }

    internal Func<IReadOnlyDictionary<string, string?>, ParameterBindingResult> Bind { get; }

    internal Func<IServiceProvider, RunInvocation, CancellationToken, Task<JobResult>> Execute { get; }

    /// <summary>Non <c>null</c> si le job implémente <see cref="IInputIdentityProvider{TParams}"/>.</summary>
    internal Func<IServiceProvider, object, CancellationToken, ValueTask<string>>? ComputeInputIdentity { get; }

    internal static JobDescriptor ForTask<TJob, TParams>(string name)
        where TJob : class, IBatchJob<TParams>
        where TParams : class, new() =>
        new(
            name,
            typeof(TJob),
            typeof(TParams),
            isChunked: false,
            ParameterBinder.Bind<TParams>,
            static (services, invocation, cancellationToken) =>
            {
                var job = services.GetRequiredService<TJob>();
                return job.ExecuteAsync(invocation.CreateContext<TParams>(), cancellationToken);
            },
            InputIdentityOf<TJob, TParams>());

    internal static JobDescriptor ForChunked<TJob, TParams, TIn, TOut>(string name)
        where TJob : class, IChunkedJob<TParams, TIn, TOut>
        where TParams : class, new()
        where TIn : notnull
        where TOut : class =>
        new(
            name,
            typeof(TJob),
            typeof(TParams),
            isChunked: true,
            ParameterBinder.Bind<TParams>,
            static (services, invocation, cancellationToken) =>
            {
                var job = services.GetRequiredService<TJob>();
                var engine = services.GetRequiredService<ChunkEngine>();
                return engine.RunAsync<TParams, TIn, TOut>(job, invocation.CreateContext<TParams>(), invocation, cancellationToken);
            },
            InputIdentityOf<TJob, TParams>());

    private static Func<IServiceProvider, object, CancellationToken, ValueTask<string>>? InputIdentityOf<TJob, TParams>()
        where TJob : class
        where TParams : class, new() =>
        typeof(IInputIdentityProvider<TParams>).IsAssignableFrom(typeof(TJob))
            ? static (services, parameters, cancellationToken) =>
                ((IInputIdentityProvider<TParams>)services.GetRequiredService<TJob>())
                    .ComputeInputIdentityAsync((TParams)parameters, cancellationToken)
            : null;
}
