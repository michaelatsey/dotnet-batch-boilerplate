namespace Batch.Abstractions;

/// <summary>
/// Erreur fonctionnelle : la donnée ou la règle métier empêche de continuer.
/// Jamais retentée. Code retour <see cref="ExitCodes.FunctionalError"/>.
/// </summary>
public class BatchFunctionalException : Exception
{
    public BatchFunctionalException()
    {
    }

    public BatchFunctionalException(string message)
        : base(message)
    {
    }

    public BatchFunctionalException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Le nombre d'items rejetés a dépassé le seuil configuré (SkipLimit).</summary>
public sealed class SkipLimitExceededException : BatchFunctionalException
{
    public SkipLimitExceededException(long skipped, int limit)
        : base($"Skip limit exceeded: {skipped} items rejected, limit is {limit}.")
    {
        Skipped = skipped;
        Limit = limit;
    }

    public long Skipped { get; }

    public int Limit { get; }
}

/// <summary>
/// Levée par <c>ProcessAsync</c> pour rejeter un item invalide sans arrêter le job.
/// Le moteur la compte, la journalise, et poursuit tant que le seuil n'est pas atteint.
/// </summary>
public sealed class SkippableItemException : Exception
{
    public SkippableItemException()
    {
    }

    public SkippableItemException(string message)
        : base(message)
    {
    }

    public SkippableItemException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
