using System.Collections.Concurrent;

namespace Batch.Core.Locking;

/// <summary>Verrou d'exécution. Implémentation distribuée en production (advisory lock PostgreSQL).</summary>
public interface IJobLockProvider
{
    /// <summary>Tente de prendre le verrou sans attendre. Retourne <c>null</c> s'il est déjà détenu.</summary>
    ValueTask<IJobLock?> TryAcquireAsync(string key, CancellationToken cancellationToken);
}

public interface IJobLock : IAsyncDisposable
{
    /// <summary>Signalé si le verrou est perdu en cours de route (coupure de la connexion qui le porte).</summary>
    CancellationToken LostToken { get; }
}

/// <summary>Verrou local au processus : suffisant en développement et pour un hôte Quartz unique.</summary>
public sealed class InMemoryJobLockProvider : IJobLockProvider
{
    private readonly ConcurrentDictionary<string, byte> _held = new(StringComparer.Ordinal);

    public ValueTask<IJobLock?> TryAcquireAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IJobLock? handle = _held.TryAdd(key, 0) ? new Handle(this, key) : null;
        return ValueTask.FromResult(handle);
    }

    private sealed class Handle(InMemoryJobLockProvider owner, string key) : IJobLock
    {
        private int _released;

        public CancellationToken LostToken => CancellationToken.None;

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                owner._held.TryRemove(key, out _);
            }

            return ValueTask.CompletedTask;
        }
    }
}
