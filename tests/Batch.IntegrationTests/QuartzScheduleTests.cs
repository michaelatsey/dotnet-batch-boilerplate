using Batch.Core.Registration;
using Batch.Hosting.Quartz;
using Batch.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Quartz;

namespace Batch.IntegrationTests;

/// <summary>Planifications déclaratives : ce qui est écrit dans la configuration est ce que Quartz exécute.</summary>
public sealed class QuartzScheduleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("30 2 * * 1-5", "0 30 2 ? * MON-FRI")] // Unix : jours 1-5 = lundi-vendredi, comme crontab
    [InlineData("0 30 2 * * ?", "0 30 2 * * ?")]       // Quartz : inchangé
    public async Task Cron_is_accepted_in_quartz_and_unix_formats(string configured, string expected)
    {
        using var host = BuildHost(configured, "Europe/Paris");
        var scheduler = await host.Services.GetRequiredService<ISchedulerFactory>().GetScheduler(Ct);

        var trigger = Assert.IsAssignableFrom<ICronTrigger>(await scheduler.GetTrigger(new TriggerKey("nightly-trigger", "batch"), Ct));

        Assert.Equal(expected, trigger.CronExpressionString);
        Assert.Equal("Europe/Paris", trigger.TimeZone.Id);
    }

    [Theory]
    [InlineData("every night", null, "neither a Quartz cron")]
    [InlineData("0 30 2 * * ?", "Europe/Pariss", "TimeZone 'Europe/Pariss' is unknown")]
    public void Invalid_schedule_fails_at_startup_with_an_actionable_message(string cron, string? timeZone, string expected)
    {
        var error = Assert.Throws<InvalidOperationException>(() => BuildHost(cron, timeZone));
        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }

    private static IHost BuildHost(string cron, string? timeZone)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Batch:Schedules:nightly:Job"] = "purge-executions",
            ["Batch:Schedules:nightly:Cron"] = cron,
            ["Batch:Schedules:nightly:TimeZone"] = timeZone,
        });
        builder.Services.AddBatch().AddBatchJobs();
        builder.AddBatchQuartzScheduler();
        return builder.Build();
    }
}
