using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Batch.Abstractions;
using Npgsql;
using NpgsqlTypes;

namespace Batch.Jobs.ImportInvoices;

/// <summary>
/// <c>batch run import-invoices -p InputFile=/data/in/invoices.csv</c>
/// </summary>
public sealed class ImportInvoicesParameters
{
    [Required]
    public string InputFile { get; set; } = "";

    [Required]
    [StringLength(1, MinimumLength = 1)]
    public string Delimiter { get; set; } = ";";

    public bool HasHeader { get; set; } = true;
}

/// <summary>Une ligne brute du fichier, avec sa position : c'est elle qui sert de checkpoint.</summary>
public sealed record InvoiceLine(long LineNumber, string Raw);

public sealed record Invoice(long LineNumber, string InvoiceNumber, string CustomerId, DateOnly IssueDate, decimal Amount, string Currency);

/// <summary>
/// Import CSV → PostgreSQL. Démontre le contrat complet d'un job par chunks :
/// lecture en flux reprenable, validation avec rejets bornés, écriture bulk idempotente.
/// </summary>
public sealed class ImportInvoicesJob(NpgsqlDataSource dataSource)
    : IChunkedJob<ImportInvoicesParameters, InvoiceLine, Invoice>, IInputIdentityProvider<ImportInvoicesParameters>
{
    private const int ExpectedFields = 5;

    // ── Identité de l'entrée ──────────────────────────────────────────────────
    // Le chemin ne suffit pas : un ordonnanceur dépose souvent chaque nuit un nouveau fichier au même endroit.
    // L'empreinte du contenu empêche d'appliquer le checkpoint d'un fichier à un autre, et distingue
    // « même fichier, déjà importé » de « nouveau fichier ». Coût : une lecture séquentielle de plus.
    public async ValueTask<string> ComputeInputIdentityAsync(ImportInvoicesParameters parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        await using var stream = new FileStream(
            parameters.InputFile,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1 << 16,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        var digest = await SHA256.HashDataAsync(stream, cancellationToken);
        return "sha256:" + Convert.ToHexStringLower(digest);
    }

    // ── Lecture ────────────────────────────────────────────────────────────────
    public async IAsyncEnumerable<InvoiceLine> ReadAsync(
        JobContext<ImportInvoicesParameters> context,
        string? resumeAfter,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        var path = context.Parameters.InputFile;
        if (!File.Exists(path))
        {
            // Fichier absent : relancer sans intervention ne changera rien.
            throw new BatchFunctionalException($"Input file not found: {path}");
        }

        var resumeAfterLine = resumeAfter is null ? 0L : long.Parse(resumeAfter, CultureInfo.InvariantCulture);
        long lineNumber = 0;

        await foreach (var line in File.ReadLinesAsync(path, cancellationToken))
        {
            lineNumber++;
            if ((lineNumber == 1 && context.Parameters.HasHeader) || lineNumber <= resumeAfterLine || string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            yield return new InvoiceLine(lineNumber, line);
        }
    }

    public string CheckpointOf(InvoiceLine item)
    {
        ArgumentNullException.ThrowIfNull(item);
        return item.LineNumber.ToString(CultureInfo.InvariantCulture);
    }

    // ── Traitement (pur, sans I/O : parallélisable) ───────────────────────────
    public ValueTask<Invoice?> ProcessAsync(InvoiceLine item, JobContext<ImportInvoicesParameters> context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(context);

        var fields = item.Raw.Split(context.Parameters.Delimiter[0]);
        if (fields.Length != ExpectedFields)
        {
            throw Reject(item, $"expected {ExpectedFields} fields, got {fields.Length}");
        }

        var invoiceNumber = fields[0].Trim();
        var customerId = fields[1].Trim();
        if (invoiceNumber.Length == 0 || customerId.Length == 0)
        {
            throw Reject(item, "invoice number and customer id are required");
        }

        if (!DateOnly.TryParseExact(fields[2].Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var issueDate))
        {
            throw Reject(item, $"invalid issue date '{fields[2]}' (expected yyyy-MM-dd)");
        }

        if (!decimal.TryParse(fields[3].Trim(), NumberStyles.Number, CultureInfo.InvariantCulture, out var amount) || amount < 0)
        {
            throw Reject(item, $"invalid amount '{fields[3]}'");
        }

        var currency = fields[4].Trim().ToUpperInvariant();
        if (currency.Length != 3)
        {
            throw Reject(item, $"invalid currency '{fields[4]}'");
        }

        return ValueTask.FromResult<Invoice?>(
            new Invoice(item.LineNumber, invoiceNumber, customerId, issueDate, decimal.Round(amount, 2), currency));
    }

    // ── Écriture : COPY binaire dans une table temporaire, puis upsert ────────
    public async Task WriteAsync(IReadOnlyList<Invoice> chunk, JobContext<ImportInvoicesParameters> context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chunk);
        ArgumentNullException.ThrowIfNull(context);

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var create = new NpgsqlCommand(
            """
            CREATE TEMP TABLE invoice_stage (
                line_number bigint, invoice_number text, customer_id text,
                issue_date date, amount numeric(14,2), currency text
            ) ON COMMIT DROP
            """,
            connection,
            transaction))
        {
            await create.ExecuteNonQueryAsync(cancellationToken);
        }

        // COPY binaire : un aller-retour réseau pour tout le chunk, sans parsing SQL ligne à ligne.
        await using (var importer = await connection.BeginBinaryImportAsync(
            "COPY invoice_stage (line_number, invoice_number, customer_id, issue_date, amount, currency) FROM STDIN (FORMAT BINARY)",
            cancellationToken))
        {
            foreach (var invoice in chunk)
            {
                await importer.StartRowAsync(cancellationToken);
                await importer.WriteAsync(invoice.LineNumber, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(invoice.InvoiceNumber, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(invoice.CustomerId, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(invoice.IssueDate, NpgsqlDbType.Date, cancellationToken);
                await importer.WriteAsync(invoice.Amount, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(invoice.Currency, NpgsqlDbType.Text, cancellationToken);
            }

            await importer.CompleteAsync(cancellationToken);
        }

        // Upsert sur la clé naturelle : rejouer un chunk (reprise, retry) ne crée aucun doublon.
        // DISTINCT ON : si le fichier contient deux fois la même facture, la dernière ligne gagne.
        await using (var upsert = new NpgsqlCommand(
            """
            INSERT INTO billing.invoice (invoice_number, customer_id, issue_date, amount, currency, source_run_id)
            SELECT DISTINCT ON (invoice_number) invoice_number, customer_id, issue_date, amount, currency, @run_id
            FROM invoice_stage
            ORDER BY invoice_number, line_number DESC
            ON CONFLICT (invoice_number) DO UPDATE SET
                customer_id   = EXCLUDED.customer_id,
                issue_date    = EXCLUDED.issue_date,
                amount        = EXCLUDED.amount,
                currency      = EXCLUDED.currency,
                source_run_id = EXCLUDED.source_run_id,
                updated_at    = now()
            """,
            connection,
            transaction))
        {
            upsert.Parameters.AddWithValue("run_id", NpgsqlDbType.Uuid, context.RunId);
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static SkippableItemException Reject(InvoiceLine item, string reason) =>
        new($"line {item.LineNumber}: {reason}");
}
