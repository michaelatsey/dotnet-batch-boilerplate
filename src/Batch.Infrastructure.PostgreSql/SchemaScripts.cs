using System.Reflection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Batch.Infrastructure.PostgreSql;

/// <summary>Script DDL idempotent contribué par un module (journal, tables métier d'un job...).</summary>
public sealed record SchemaScript(string Name, int Order, string Sql)
{
    public static SchemaScript FromEmbeddedResource(Assembly assembly, string logicalName, int order)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        using var stream = assembly.GetManifestResourceStream(logicalName)
            ?? throw new InvalidOperationException($"Embedded resource '{logicalName}' not found in {assembly.GetName().Name}.");
        using var reader = new StreamReader(stream);
        return new SchemaScript(logicalName, order, reader.ReadToEnd());
    }
}

/// <summary>Applique tous les scripts enregistrés, dans l'ordre, dans une seule transaction.</summary>
public sealed partial class SchemaInitializer(
    NpgsqlDataSource dataSource,
    IEnumerable<SchemaScript> scripts,
    ILogger<SchemaInitializer> logger)
{
    public async Task<int> ApplyAsync(CancellationToken cancellationToken)
    {
        var ordered = scripts.OrderBy(s => s.Order).ThenBy(s => s.Name, StringComparer.Ordinal).ToList();

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var script in ordered)
        {
            await using var command = new NpgsqlCommand(script.Sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken);
            LogApplied(logger, script.Name);
        }

        await transaction.CommitAsync(cancellationToken);
        return ordered.Count;
    }

    [LoggerMessage(3000, LogLevel.Information, "Applied schema script {Name}.")]
    private static partial void LogApplied(ILogger logger, string name);
}
