namespace Batch.Abstractions;

/// <summary>Résultat fonctionnel retourné par un job qui n'a pas levé d'exception.</summary>
public sealed record JobResult(JobStatus Status, string? Summary = null)
{
    public static JobResult Success(string? summary = null) => new(JobStatus.Succeeded, summary);

    /// <summary>Le job a abouti mais des éléments ont été rejetés ou ignorés : code retour 1.</summary>
    public static JobResult CompletedWithWarnings(string summary) => new(JobStatus.CompletedWithWarnings, summary);

    public static JobResult NothingToDo(string? summary = null) => new(JobStatus.Succeeded, summary ?? "Nothing to do");
}

/// <summary>Statut persistant d'une exécution dans le journal.</summary>
public enum JobStatus
{
    Running,
    Succeeded,
    CompletedWithWarnings,
    FunctionalError,
    TechnicalError,
    Cancelled,
    TimedOut,

    /// <summary>Processus mort sans finaliser (kill -9, OOM, perte du nœud). Détecté à la prise du verrou suivante.</summary>
    Abandoned,
}

public static class JobStatusExtensions
{
    public static bool IsSuccessful(this JobStatus status) =>
        status is JobStatus.Succeeded or JobStatus.CompletedWithWarnings;

    public static bool IsTerminal(this JobStatus status) => status is not JobStatus.Running;
}
