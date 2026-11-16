using System.Data.Common;
using Batch.Abstractions;

namespace Batch.Core.Resilience;

public static class BatchResilience
{
    /// <summary>Pipeline Polly appliqué au traitement et à l'écriture des chunks.</summary>
    public const string PipelineName = "batch";
}

/// <summary>
/// Décide si une erreur vaut un nouvel essai. Remplaçable en DI pour reconnaître les erreurs d'un SDK tiers.
/// </summary>
public interface ITransientErrorClassifier
{
    bool IsTransient(Exception exception);
}

public sealed class DefaultTransientErrorClassifier : ITransientErrorClassifier
{
    public bool IsTransient(Exception exception) => exception switch
    {
        // HttpClient.Timeout : TaskCanceledException causée par un TimeoutException. Ce n'est pas une annulation
        // demandée, c'est un appel lent : à retenter. (Une vraie annulation du run arrête Polly de toute façon.)
        TaskCanceledException { InnerException: TimeoutException } => true,

        // Jamais retentées : la donnée est fausse ou l'arrêt est demandé.
        BatchFunctionalException or SkippableItemException or OperationCanceledException => false,

        TimeoutException => true,

        // Npgsql, SqlClient... exposent leur propre jugement via DbException.IsTransient.
        DbException db => db.IsTransient,

        HttpRequestException => true,
        IOException => true,

        { InnerException: { } inner } => IsTransient(inner),
        _ => false,
    };
}
