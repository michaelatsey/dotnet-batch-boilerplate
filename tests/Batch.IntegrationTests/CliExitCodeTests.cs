using Batch.Abstractions;
using Batch.Host.Cli;

namespace Batch.IntegrationTests;

/// <summary>
/// Le code retour vu par l'ordonnanceur, de bout en bout : parsing, construction de l'hôte, commande.
/// La configuration passe par les variables d'environnement, comme en production : collection non parallélisée.
/// </summary>
[Collection(CliCollection.Name)]
public sealed class CliExitCodeTests : IDisposable
{
    // Port 1 : connexion refusée immédiatement, sans attendre un timeout réseau.
    private const string UnreachableDatabase = "Host=127.0.0.1;Port=1;Timeout=2;Command Timeout=2;Pooling=false";

    private readonly List<string> _variables = [];

    [Fact]
    public async Task Parse_error_is_invalid_usage_not_warnings()
    {
        Assert.Equal(ExitCodes.InvalidUsage, await CliApp.InvokeAsync(["run"]));
    }

    [Fact]
    public async Task Run_against_an_unreachable_database_is_a_technical_error()
    {
        Set("ConnectionStrings__Batch", UnreachableDatabase);

        Assert.Equal(ExitCodes.TechnicalError, await CliApp.InvokeAsync(["run", "import-invoices", "-p", "InputFile=/tmp/x.csv"]));
    }

    [Fact]
    public async Task History_against_an_unreachable_database_is_a_technical_error()
    {
        Set("ConnectionStrings__Batch", UnreachableDatabase);

        Assert.Equal(ExitCodes.TechnicalError, await CliApp.InvokeAsync(["history"]));
    }

    [Fact]
    public async Task Run_of_a_database_job_without_connection_string_is_a_configuration_error()
    {
        Assert.Equal(ExitCodes.ConfigurationError, await CliApp.InvokeAsync(["run", "import-invoices", "-p", "InputFile=/tmp/x.csv"]));
    }

    [Theory]
    [InlineData("every night", null)]              // ni Quartz ni Unix
    [InlineData("0 30 2 * * ?", "Europe/Pariss")]  // fuseau inconnu
    public async Task Serve_with_an_invalid_schedule_is_a_configuration_error(string cron, string? timeZone)
    {
        Set("Batch__Schedules__bad__Job", "purge-executions");
        Set("Batch__Schedules__bad__Cron", cron);
        if (timeZone is not null)
        {
            Set("Batch__Schedules__bad__TimeZone", timeZone);
        }

        Assert.Equal(ExitCodes.ConfigurationError, await CliApp.InvokeAsync(["serve"]));
    }

    public void Dispose()
    {
        foreach (var name in _variables)
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    private void Set(string name, string value)
    {
        _variables.Add(name);
        Environment.SetEnvironmentVariable(name, value);
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class CliCollection
{
    public const string Name = "cli";
}
