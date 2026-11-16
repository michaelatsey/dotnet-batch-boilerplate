namespace Batch.Core;

/// <summary>Options globales du moteur. Section <c>Batch</c>.</summary>
public sealed class BatchOptions
{
    public const string SectionName = "Batch";

    /// <summary>Fréquence de mise à jour du heartbeat et des compteurs dans le journal.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Temps accordé à la finalisation (écriture du statut final), même après annulation.</summary>
    public TimeSpan FinalizationTimeout { get; set; } = TimeSpan.FromSeconds(15);

    public RetrySettings Retry { get; set; } = new();
}

/// <summary>Retry exponentiel avec jitter, appliqué uniquement aux erreurs transitoires.</summary>
public sealed class RetrySettings
{
    /// <summary>Nombre de nouvelles tentatives. 0 désactive le retry.</summary>
    public int MaxAttempts { get; set; } = 3;

    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Options d'un job. Valeurs par défaut posées dans le code à l'enregistrement,
/// surchargeables par l'exploitation dans <c>Batch:Jobs:&lt;nom&gt;</c> sans redéploiement.
/// </summary>
public sealed class BatchJobOptions
{
    public string? Description { get; set; }

    /// <summary>Nombre d'items par chunk : une transaction et un checkpoint par chunk.</summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>Parallélisme de l'étape de traitement. L'écriture reste séquentielle et ordonnée.</summary>
    public int MaxDegreeOfParallelism { get; set; } = 1;

    /// <summary>Nombre de chunks en attente entre deux étapes (contre-pression).</summary>
    public int PipelineDepth { get; set; } = 2;

    /// <summary>Nombre maximal d'items rejetés avant d'échouer en erreur fonctionnelle.</summary>
    public int SkipLimit { get; set; }

    /// <summary>Durée maximale d'exécution. Au-delà, annulation et code retour 6.</summary>
    public TimeSpan? Timeout { get; set; }

    public ConcurrencyMode Concurrency { get; set; } = ConcurrencyMode.SingleInstancePerParameters;
}

public enum ConcurrencyMode
{
    /// <summary>Une seule exécution du job à la fois, quels que soient ses paramètres.</summary>
    SingleInstance,

    /// <summary>Une seule exécution par jeu de paramètres : deux dates différentes peuvent tourner en parallèle.</summary>
    SingleInstancePerParameters,

    /// <summary>Aucun verrou. Réservé aux jobs naturellement idempotents et sans effet de bord concurrent.</summary>
    Unrestricted,
}
