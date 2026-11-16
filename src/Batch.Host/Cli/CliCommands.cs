using System.CommandLine;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Batch.Abstractions;
using Batch.Core;
using Batch.Core.Execution;
using Batch.Core.Persistence;
using Batch.Core.Registration;
using Batch.Hosting.Quartz;
using Batch.Infrastructure.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Batch.Host.Cli;

internal static class CliCommands
{
    public static readonly TimeSpan TerminationGracePeriod = ReadGracePeriod();

    // ── batch run <job> [-p key=value]... [--restart] ──────────────────────────
    public static Command Run()
    {
        var job = new Argument<string>("job") { Description = "Job name (see 'list')." };
        var parameters = new Option<string[]>("--param", "-p")
        {
            Description = "Job parameter as key=value. Repeatable. Nested keys use ':' (Section:Key=value).",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var restart = new Option<bool>("--restart")
        {
            Description = "Resume the last failed execution with the same parameters from its checkpoint. " +
                          "No-op (exit 0) if it already succeeded.",
        };

        var command = new Command("run", "Run one job and exit with its status code.") { job, parameters, restart };
        command.SetAction((parse, cancellationToken) => RunJobAsync(
            parse.GetValue(job)!,
            parse.GetValue(parameters) ?? [],
            parse.GetValue(restart),
            cancellationToken));
        return command;
    }

    // ── batch list ─────────────────────────────────────────────────────────────
    public static Command List()
    {
        var command = new Command("list", "List registered jobs and their parameters.");
        command.SetAction((parse, cancellationToken) => WithHostAsync(HostMode.Command, (host, token) =>
        {
            var registry = host.Services.GetRequiredService<IJobRegistry>();
            var options = host.Services.GetRequiredService<IOptionsMonitor<BatchJobOptions>>();

            foreach (var descriptor in registry.Jobs.OrderBy(d => d.Name, StringComparer.Ordinal))
            {
                var settings = options.Get(descriptor.Name);
                Console.Out.WriteLine($"{descriptor.Name}  [{(descriptor.IsChunked ? "chunked" : "task")}]  {settings.Description}");
                foreach (var property in descriptor.ParametersType.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite))
                {
                    var required = property.GetCustomAttribute<RequiredAttribute>() is not null ? " (required)" : "";
                    Console.Out.WriteLine($"    -p {property.Name}=<{property.PropertyType.Name}>{required}");
                }
            }

            return Task.FromResult(ExitCodes.Success);
        }, cancellationToken));
        return command;
    }

    // ── batch history [--job name] [--take n] ──────────────────────────────────
    public static Command History()
    {
        var job = new Option<string?>("--job") { Description = "Filter by job name." };
        var take = new Option<int>("--take") { Description = "Number of executions.", DefaultValueFactory = _ => 20 };

        var command = new Command("history", "Show the most recent executions from the journal.") { job, take };
        command.SetAction((parse, cancellationToken) => WithHostAsync(HostMode.Command, async (host, token) =>
        {
            var store = host.Services.GetRequiredService<IJobExecutionStore>();
            var records = await store.ListRecentAsync(parse.GetValue(job), Math.Clamp(parse.GetValue(take), 1, 500), token);

            foreach (var r in records)
            {
                var ended = r.EndedAt?.ToString("u", CultureInfo.InvariantCulture) ?? "-";
                Console.Out.WriteLine(
                    $"{r.RunId}  {r.JobName,-20} {r.Status,-22} exit={r.ExitCode?.ToString(CultureInfo.InvariantCulture) ?? "-",-3} " +
                    $"start={r.StartedAt.ToString("u", CultureInfo.InvariantCulture)} end={ended} " +
                    $"read={r.ReadCount} written={r.WriteCount} skipped={r.SkipCount}");
            }

            return ExitCodes.Success;
        }, cancellationToken));
        return command;
    }

    // ── batch serve ────────────────────────────────────────────────────────────
    public static Command Serve()
    {
        var command = new Command("serve", "Run the embedded Quartz scheduler until SIGTERM. Schedules come from Batch:Schedules.");
        command.SetAction((parse, cancellationToken) => WithHostAsync(HostMode.Scheduler, async (host, token) =>
        {
            // Une planification qui vise un job inexistant est une erreur de configuration, pas un échec nocturne.
            var registry = host.Services.GetRequiredService<IJobRegistry>();
            var schedules = QuartzHostingExtensions.ReadSchedules(host.Services.GetRequiredService<IConfiguration>());
            var unknown = schedules.Where(s => !registry.TryGet(s.Job, out _)).Select(s => $"{s.Name} -> {s.Job}").ToList();
            if (unknown.Count > 0)
            {
                await Console.Error.WriteLineAsync($"Schedules reference unknown jobs: {string.Join(", ", unknown)}");
                return ExitCodes.ConfigurationError;
            }

            await host.WaitForShutdownAsync(token);
            return ExitCodes.Success;
        }, cancellationToken));
        return command;
    }

    // ── batch db init ──────────────────────────────────────────────────────────
    public static Command Db()
    {
        var init = new Command("init", "Apply idempotent schema scripts (journal + job tables). Requires ConnectionStrings:Batch.");
        init.SetAction((parse, cancellationToken) => WithHostAsync(HostMode.Command, async (host, token) =>
        {
            var initializer = host.Services.GetService<SchemaInitializer>();
            if (initializer is null)
            {
                await Console.Error.WriteLineAsync($"ConnectionStrings:{BatchHostFactory.ConnectionStringName} is not configured.");
                return ExitCodes.ConfigurationError;
            }

            var count = await initializer.ApplyAsync(token);
            await Console.Out.WriteLineAsync($"Applied {count} schema script(s).");
            return ExitCodes.Success;
        }, cancellationToken));

        return new Command("db", "Database administration.") { init };
    }

    private static Task<int> RunJobAsync(string job, string[] rawParameters, bool restart, CancellationToken cancellationToken)
    {
        if (!TryParseParameters(rawParameters, out var parameters, out var error))
        {
            Console.Error.WriteLine(error);
            return Task.FromResult(ExitCodes.InvalidUsage);
        }

        return WithHostAsync(HostMode.Command, async (host, token) =>
        {
            var runner = host.Services.GetRequiredService<IJobRunner>();
            var outcome = await runner.RunAsync(new RunRequest(job, parameters, JobTrigger.Cli, restart), token);

            var summary = new RunSummary(
                job,
                outcome.RunId,
                outcome.Status?.ToString() ?? "Rejected",
                outcome.ExitCode,
                outcome.Counters.Read,
                outcome.Counters.Written,
                outcome.Counters.Skipped,
                outcome.Message);
            await Console.Out.WriteLineAsync(JsonSerializer.Serialize(summary, CliJsonContext.Default.RunSummary));

            return outcome.ExitCode;
        }, cancellationToken);
    }

    /// <summary>Construit, démarre et arrête l'hôte autour d'une commande, et traduit les erreurs de configuration.</summary>
    private static async Task<int> WithHostAsync(HostMode mode, Func<IHost, CancellationToken, Task<int>> action, CancellationToken cancellationToken)
    {
        IHost host;
        try
        {
            host = BatchHostFactory.Build(mode);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            await Console.Error.WriteLineAsync($"Configuration error: {ex.Message}");
            return ExitCodes.ConfigurationError;
        }

        using (host)
        {
            try
            {
                // Le démarrage valide les options (ValidateOnStart) : une configuration fausse échoue ici, en millisecondes.
                await host.StartAsync(cancellationToken);
            }
            catch (OptionsValidationException ex)
            {
                await Console.Error.WriteLineAsync($"Configuration error: {string.Join("; ", ex.Failures)}");
                return ExitCodes.ConfigurationError;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return ExitCodes.Cancelled;
            }
            catch (Exception ex)
            {
                // Typiquement : JobStore Quartz clusterisé injoignable au démarrage de `serve`.
                await Console.Error.WriteLineAsync($"Technical error while starting: {ex.GetType().Name}: {ex.Message}");
                return ExitCodes.TechnicalError;
            }

            // Double sécurité : l'arrêt de l'hôte (SIGTERM capté par sa ConsoleLifetime) annule aussi la commande.
            var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lifetime.ApplicationStopping);

            try
            {
                return await action(host, linked.Token);
            }
            catch (OperationCanceledException) when (linked.IsCancellationRequested)
            {
                return ExitCodes.Cancelled;
            }
            catch (Exception ex)
            {
                // `history`, `db init`... contre une base tombée : erreur technique, relance possible.
                await Console.Error.WriteLineAsync($"Technical error: {ex.GetType().Name}: {ex.Message}");
                return ExitCodes.TechnicalError;
            }
            finally
            {
                await StopAsync(host);
            }
        }
    }

    /// <summary>Arrêt avec un jeton neuf : vide les exporteurs OpenTelemetry même après annulation.</summary>
    /// <remarks>Ne lève jamais : une exception ici écraserait le code retour déjà calculé.</remarks>
    private static async Task StopAsync(IHost host)
    {
        try
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(stopCts.Token);
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"Warning: host shutdown failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    internal static bool TryParseParameters(string[] raw, out Dictionary<string, string?> parameters, out string? error)
    {
        parameters = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in raw)
        {
            var separator = token.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                error = $"Invalid parameter '{token}': expected key=value.";
                return false;
            }

            var key = token[..separator].Trim();
            if (!parameters.TryAdd(key, token[(separator + 1)..]))
            {
                error = $"Parameter '{key}' is specified more than once.";
                return false;
            }
        }

        error = null;
        return true;
    }

    private static TimeSpan ReadGracePeriod()
    {
        var raw = Environment.GetEnvironmentVariable("BATCH_TERMINATION_GRACE_SECONDS");
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds) && seconds > 0
            ? TimeSpan.FromSeconds(seconds)
            : TimeSpan.FromSeconds(60);
    }
}

internal sealed record RunSummary(
    string Job,
    Guid? RunId,
    string Status,
    int ExitCode,
    long Read,
    long Written,
    long Skipped,
    string Message);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(RunSummary))]
internal sealed partial class CliJsonContext : JsonSerializerContext
{
}
