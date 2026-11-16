namespace Batch.Abstractions;

/// <summary>Marqueur non générique, utilisé uniquement par le registre de jobs.</summary>
public interface IBatchJob;

/// <summary>
/// Job « tâche » : une unité de travail exécutée d'un bloc (purge, appel d'API, export simple).
/// Le job ne sait pas qui l'a déclenché : CLI (VTOM, Control-M, cron, Kubernetes) ou Quartz.
/// </summary>
/// <typeparam name="TParams">Paramètres typés, liés depuis la ligne de commande ou le JobDataMap Quartz.</typeparam>
public interface IBatchJob<TParams> : IBatchJob
    where TParams : class, new()
{
    Task<JobResult> ExecuteAsync(JobContext<TParams> context, CancellationToken cancellationToken);
}

/// <summary>
/// Job « par chunks » : lecture en flux, traitement item par item, écriture par lots.
/// Le moteur gère pour lui le découpage, le parallélisme borné, les rejets, les retries et le checkpoint.
/// </summary>
/// <remarks>
/// Contrat d'idempotence : <see cref="WriteAsync"/> doit pouvoir rejouer un chunk sans effet de bord
/// (upsert, clé naturelle). La reprise est « au moins une fois » : un chunk écrit dont le checkpoint
/// n'a pas eu le temps d'être enregistré sera réécrit au redémarrage.
/// </remarks>
public interface IChunkedJob<TParams, TIn, TOut> : IBatchJob
    where TParams : class, new()
    where TIn : notnull
    where TOut : class
{
    /// <summary>
    /// Lit la source en flux, en reprenant <b>après</b> <paramref name="resumeAfter"/> s'il est fourni.
    /// Utiliser une pagination par clé (keyset), jamais OFFSET.
    /// </summary>
    IAsyncEnumerable<TIn> ReadAsync(JobContext<TParams> context, string? resumeAfter, CancellationToken cancellationToken);

    /// <summary>
    /// Transforme un item. Retourner <c>null</c> pour le filtrer.
    /// Lever <see cref="SkippableItemException"/> pour le rejeter (compté, borné par SkipLimit).
    /// Peut être appelé en parallèle : ne pas partager d'état mutable.
    /// </summary>
    ValueTask<TOut?> ProcessAsync(TIn item, JobContext<TParams> context, CancellationToken cancellationToken);

    /// <summary>Écrit un chunk dans une seule transaction. Appelé séquentiellement, dans l'ordre de lecture.</summary>
    Task WriteAsync(IReadOnlyList<TOut> chunk, JobContext<TParams> context, CancellationToken cancellationToken);

    /// <summary>Position de reprise correspondant à un item lu (clé, numéro de ligne, horodatage...).</summary>
    string CheckpointOf(TIn item);
}
