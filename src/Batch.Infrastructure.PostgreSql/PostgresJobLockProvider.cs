using Batch.Core.Locking;
using Medallion.Threading.Postgres;

namespace Batch.Infrastructure.PostgreSql;

/// <summary>
/// Verrou distribué par advisory lock PostgreSQL (session). Libéré automatiquement par le serveur
/// si le processus meurt : aucun verrou orphelin, aucun TTL à régler.
/// </summary>
/// <remarks>
/// La connexion doit être une vraie session : pas de PgBouncer en mode <c>transaction</c> sur cette chaîne.
/// </remarks>
internal sealed class PostgresJobLockProvider(string connectionString) : IJobLockProvider
{
    public async ValueTask<IJobLock?> TryAcquireAsync(string key, CancellationToken cancellationToken)
    {
        var distributedLock = new PostgresDistributedLock(new PostgresAdvisoryLockKey(key, allowHashing: true), connectionString);
        var handle = await distributedLock.TryAcquireAsync(TimeSpan.Zero, cancellationToken);
        return handle is null ? null : new Handle(handle);
    }

    private sealed class Handle(PostgresDistributedLockHandle inner) : IJobLock
    {
        public CancellationToken LostToken => inner.HandleLostToken;

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
