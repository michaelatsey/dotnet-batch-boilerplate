using Batch.Abstractions;
using Batch.Core.Registration;
using Batch.Jobs.ImportInvoices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using static Batch.Core.Tests.TestSupport;

namespace Batch.Core.Tests;

public sealed class RegistrationTests
{
    [Theory]
    [InlineData("Import")]
    [InlineData("import_invoices")]
    [InlineData("-import")]
    [InlineData("")]
    public void Job_names_must_be_kebab_case(string name)
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddBatch().AddJob<ScriptedJob, EchoParameters>(name));
    }

    [Fact]
    public void Duplicate_job_names_are_rejected_at_startup()
    {
        using var sp = Build(b => b
            .AddJob<ScriptedJob, EchoParameters>("same")
            .AddJob<ScriptedJob, EchoParameters>("same"),
            services: s => s.AddSingleton(new ScriptedBehavior()));

        Assert.Throws<InvalidOperationException>(() => sp.GetRequiredService<IJobRegistry>());
    }

    [Fact]
    public void Exit_codes_are_a_stable_contract()
    {
        // Ces valeurs sont branchées dans les ordonnanceurs : les changer est un changement cassant.
        Assert.Equal(0, ExitCodes.Success);
        Assert.Equal(1, ExitCodes.CompletedWithWarnings);
        Assert.Equal(2, ExitCodes.FunctionalError);
        Assert.Equal(3, ExitCodes.TechnicalError);
        Assert.Equal(4, ExitCodes.AlreadyRunning);
        Assert.Equal(5, ExitCodes.Cancelled);
        Assert.Equal(6, ExitCodes.TimedOut);
        Assert.Equal(64, ExitCodes.InvalidUsage);
        Assert.Equal(78, ExitCodes.ConfigurationError);
    }
}

public sealed class ImportInvoicesProcessingTests
{
    // Le traitement est pur : testable sans base de données.
    private static readonly ImportInvoicesJob Job = new(dataSource: null!);

    private static readonly JobContext<ImportInvoicesParameters> Context = new()
    {
        RunId = Guid.CreateVersion7(),
        JobName = "import-invoices",
        Parameters = new ImportInvoicesParameters { InputFile = "unused.csv" },
        Trigger = JobTrigger.Test,
        Progress = new Execution.JobProgress(),
        Logger = NullLogger.Instance,
    };

    [Fact]
    public async Task Valid_line_is_parsed()
    {
        var invoice = await Job.ProcessAsync(new InvoiceLine(2, "INV-1; C-9 ;2026-05-01;1234.567;eur"), Context, Ct);

        Assert.NotNull(invoice);
        Assert.Equal("INV-1", invoice.InvoiceNumber);
        Assert.Equal("C-9", invoice.CustomerId);
        Assert.Equal(new DateOnly(2026, 5, 1), invoice.IssueDate);
        Assert.Equal(1234.57m, invoice.Amount);
        Assert.Equal("EUR", invoice.Currency);
    }

    [Theory]
    [InlineData("INV-1;C-9;2026-05-01;10.00")]          // champ manquant
    [InlineData("INV-1;C-9;2026-13-45;10.00;EUR")]      // date invalide
    [InlineData("INV-1;C-9;2026-05-01;-1;EUR")]         // montant négatif
    [InlineData("INV-1;C-9;2026-05-01;abc;EUR")]        // montant illisible
    [InlineData("INV-1;C-9;2026-05-01;10.00;EURO")]     // devise
    [InlineData(";C-9;2026-05-01;10.00;EUR")]           // numéro vide
    public async Task Invalid_line_is_skippable(string raw)
    {
        await Assert.ThrowsAsync<SkippableItemException>(async () =>
            await Job.ProcessAsync(new InvoiceLine(3, raw), Context, Ct));
    }
}
