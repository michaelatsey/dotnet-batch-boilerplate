namespace Batch.Abstractions;

/// <summary>Compteurs d'avancement, thread-safe. Persistés au heartbeat et à chaque checkpoint.</summary>
public interface IJobProgress
{
    void AddRead(long count = 1);

    void AddWritten(long count = 1);

    void AddSkipped(long count = 1);

    JobCounters Snapshot();
}

public readonly record struct JobCounters(long Read, long Written, long Skipped)
{
    public static readonly JobCounters Zero = new(0, 0, 0);
}
