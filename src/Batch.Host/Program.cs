using System.CommandLine;
using Batch.Host.Cli;

// Le contrat avec l'ordonnanceur tient en trois choses : la ligne de commande, le code retour,
// et une ligne JSON sur stdout. Les logs partent sur stderr pour ne jamais polluer ce résultat.
var root = new RootCommand(
    "Batch host. 'run' executes one job and exits with a stable code (VTOM, Control-M, cron, Kubernetes). " +
    "'serve' runs the embedded Quartz scheduler.");

root.Subcommands.Add(CliCommands.Run());
root.Subcommands.Add(CliCommands.List());
root.Subcommands.Add(CliCommands.History());
root.Subcommands.Add(CliCommands.Serve());
root.Subcommands.Add(CliCommands.Db());

var invocation = new InvocationConfiguration
{
    // SIGTERM / Ctrl+C annulent le jeton passé aux commandes. Le job dispose de ce délai pour
    // finir son chunk, écrire son checkpoint et son statut avant que le processus soit tué.
    // Kubernetes : terminationGracePeriodSeconds doit être supérieur à cette valeur.
    ProcessTerminationTimeout = CliCommands.TerminationGracePeriod,
};

return await root.Parse(args).InvokeAsync(invocation);
