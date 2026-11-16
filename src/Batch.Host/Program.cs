using Batch.Host.Cli;

// Le contrat avec l'ordonnanceur tient en trois choses : la ligne de commande, le code retour,
// et une ligne JSON sur stdout. Les logs partent sur stderr pour ne jamais polluer ce résultat.
return await CliApp.InvokeAsync(args);
