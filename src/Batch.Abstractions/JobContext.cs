using Microsoft.Extensions.Logging;

namespace Batch.Abstractions;

/// <summary>Contexte d'une exécution, transmis au job.</summary>
public sealed class JobContext<TParams>
    where TParams : class, new()
{
    /// <summary>Identifiant unique de l'exécution (UUID v7 : triable chronologiquement).</summary>
    public required Guid RunId { get; init; }

    public required string JobName { get; init; }

    public required TParams Parameters { get; init; }

    public required JobTrigger Trigger { get; init; }

    /// <summary>Exécution reprise, si ce run est un redémarrage.</summary>
    public Guid? RestartOf { get; init; }

    public required IJobProgress Progress { get; init; }

    public required ILogger Logger { get; init; }
}

/// <summary>Origine du déclenchement. Informatif : le job ne doit pas changer de comportement selon elle.</summary>
public enum JobTrigger
{
    Cli,
    Quartz,
    Test,
}
