using System.CommandLine;
using Batch.Abstractions;

namespace Batch.Host.Cli;

/// <summary>Racine de la ligne de commande. Séparée de Program.cs pour que les codes retour soient testables.</summary>
internal static class CliApp
{
    public static RootCommand BuildRoot()
    {
        var root = new RootCommand(
            "Batch host. 'run' executes one job and exits with a stable code (VTOM, Control-M, cron, Kubernetes). " +
            "'serve' runs the embedded Quartz scheduler.");

        root.Subcommands.Add(CliCommands.Run());
        root.Subcommands.Add(CliCommands.List());
        root.Subcommands.Add(CliCommands.History());
        root.Subcommands.Add(CliCommands.Serve());
        root.Subcommands.Add(CliCommands.Db());
        return root;
    }

    /// <summary>
    /// Parse et exécute. Aucun chemin ne doit produire 1 par accident : c'est le code « succès partiel »,
    /// que l'ordonnanceur laisse passer. System.CommandLine rend 1 sur erreur de parsing ou exception non gérée.
    /// </summary>
    public static async Task<int> InvokeAsync(string[] args)
    {
        var parse = BuildRoot().Parse(args);
        if (parse.Errors.Count > 0)
        {
            foreach (var error in parse.Errors)
            {
                await Console.Error.WriteLineAsync(error.Message);
            }

            await Console.Error.WriteLineAsync("Run 'batch --help' for usage.");
            return ExitCodes.InvalidUsage;
        }

        var invocation = new InvocationConfiguration
        {
            // SIGTERM / Ctrl+C annulent le jeton passé aux commandes. Le job dispose de ce délai pour
            // finir son chunk, écrire son checkpoint et son statut avant que le processus soit tué.
            // Kubernetes : terminationGracePeriodSeconds doit être supérieur à cette valeur.
            ProcessTerminationTimeout = CliCommands.TerminationGracePeriod,

            // Dernier filet : les commandes traduisent déjà leurs erreurs, mais une exception qui
            // s'échapperait ne doit jamais devenir le 1 du gestionnaire par défaut.
            EnableDefaultExceptionHandler = false,
        };

        try
        {
            return await parse.InvokeAsync(invocation);
        }
        catch (OperationCanceledException)
        {
            return ExitCodes.Cancelled;
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"Unhandled error: {ex}");
            return ExitCodes.TechnicalError;
        }
    }
}
