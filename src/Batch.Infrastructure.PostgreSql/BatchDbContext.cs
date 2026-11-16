using Batch.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace Batch.Infrastructure.PostgreSql;

/// <summary>
/// Contexte EF Core réservé au journal. Le schéma est piloté par un script SQL versionné
/// (<c>Sql/001_batch_journal.sql</c>), lisible par un DBA, plutôt que par des migrations générées.
/// </summary>
public sealed class BatchDbContext(DbContextOptions<BatchDbContext> options) : DbContext(options)
{
    public DbSet<JobExecutionEntity> Executions => Set<JobExecutionEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.HasDefaultSchema("batch");

        var execution = modelBuilder.Entity<JobExecutionEntity>();
        execution.ToTable("job_execution");
        execution.HasKey(e => e.RunId);

        execution.Property(e => e.RunId).HasColumnName("run_id").ValueGeneratedNever();
        execution.Property(e => e.JobName).HasColumnName("job_name");
        execution.Property(e => e.ParametersHash).HasColumnName("parameters_hash");
        execution.Property(e => e.ParametersJson).HasColumnName("parameters").HasColumnType("jsonb");
        execution.Property(e => e.InputIdentity).HasColumnName("input_identity");
        execution.Property(e => e.Trigger).HasColumnName("trigger_source").HasConversion<string>();
        execution.Property(e => e.HostName).HasColumnName("host_name");
        execution.Property(e => e.Status).HasColumnName("status").HasConversion<string>();
        execution.Property(e => e.StartedAt).HasColumnName("started_at");
        execution.Property(e => e.EndedAt).HasColumnName("ended_at");
        execution.Property(e => e.LastHeartbeatAt).HasColumnName("last_heartbeat_at");
        execution.Property(e => e.RestartOf).HasColumnName("restart_of");
        execution.Property(e => e.Checkpoint).HasColumnName("checkpoint");
        execution.Property(e => e.ReadCount).HasColumnName("read_count");
        execution.Property(e => e.WriteCount).HasColumnName("write_count");
        execution.Property(e => e.SkipCount).HasColumnName("skip_count");
        execution.Property(e => e.ExitCode).HasColumnName("exit_code");
        execution.Property(e => e.Message).HasColumnName("message");
    }
}

public sealed class JobExecutionEntity
{
    public Guid RunId { get; set; }

    public string JobName { get; set; } = "";

    public string ParametersHash { get; set; } = "";

    public string ParametersJson { get; set; } = "{}";

    public string? InputIdentity { get; set; }

    public JobTrigger Trigger { get; set; }

    public string HostName { get; set; } = "";

    public JobStatus Status { get; set; }

    public DateTimeOffset StartedAt { get; set; }

    public DateTimeOffset? EndedAt { get; set; }

    public DateTimeOffset LastHeartbeatAt { get; set; }

    public Guid? RestartOf { get; set; }

    public string? Checkpoint { get; set; }

    public long ReadCount { get; set; }

    public long WriteCount { get; set; }

    public long SkipCount { get; set; }

    public int? ExitCode { get; set; }

    public string? Message { get; set; }
}
