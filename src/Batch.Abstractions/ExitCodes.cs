namespace Batch.Abstractions;

/// <summary>
/// Contrat avec l'exploitation. Ces valeurs sont stables : un ordonnanceur (VTOM, Control-M, Autosys)
/// branche ses actions dessus. Toute modification est un changement cassant.
/// </summary>
public static class ExitCodes
{
    /// <summary>Succès. La chaîne continue.</summary>
    public const int Success = 0;

    /// <summary>Succès partiel : des items ont été rejetés sous le seuil toléré. Alerte, la chaîne peut continuer.</summary>
    public const int CompletedWithWarnings = 1;

    /// <summary>Erreur fonctionnelle (données invalides, règle métier). Relancer ne sert à rien sans correction.</summary>
    public const int FunctionalError = 2;

    /// <summary>Erreur technique (base, réseau, disque) après épuisement des retries. Relance automatique conseillée.</summary>
    public const int TechnicalError = 3;

    /// <summary>Une exécution avec la même clé de concurrence est déjà en cours. Ni relance, ni alerte rouge.</summary>
    public const int AlreadyRunning = 4;

    /// <summary>Arrêt demandé (SIGTERM, Ctrl+C, arrêt de pod). Relancer avec --restart.</summary>
    public const int Cancelled = 5;

    /// <summary>Le job a dépassé son timeout. Relancer avec --restart, et revoir le dimensionnement.</summary>
    public const int TimedOut = 6;

    /// <summary>Usage invalide : job inconnu, paramètre manquant ou invalide (convention sysexits EX_USAGE).</summary>
    public const int InvalidUsage = 64;

    /// <summary>Configuration invalide de l'hôte (chaîne de connexion absente...). Convention sysexits EX_CONFIG.</summary>
    public const int ConfigurationError = 78;

    public static int From(JobStatus status) => status switch
    {
        JobStatus.Succeeded => Success,
        JobStatus.CompletedWithWarnings => CompletedWithWarnings,
        JobStatus.FunctionalError => FunctionalError,
        JobStatus.Cancelled => Cancelled,
        JobStatus.TimedOut => TimedOut,
        _ => TechnicalError,
    };
}
