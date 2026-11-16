using System.Diagnostics;
using System.Diagnostics.Metrics;
using Batch.Abstractions;

namespace Batch.Core.Telemetry;

/// <summary>Noms des sources OpenTelemetry à déclarer dans l'hôte.</summary>
public static class BatchTelemetry
{
    public const string SourceName = "Batch";

    public const string MeterName = "Batch";

    internal static readonly ActivitySource ActivitySource = new(SourceName);
}

/// <summary>Métriques du moteur. Toutes taguées par <c>batch.job</c>.</summary>
public sealed class BatchMetrics
{
    private readonly Counter<long> _runs;
    private readonly Histogram<double> _runDuration;
    private readonly Counter<long> _items;
    private readonly Histogram<double> _chunkDuration;

    public BatchMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        var meter = meterFactory.Create(BatchTelemetry.MeterName);

        _runs = meter.CreateCounter<long>("batch.runs", "{run}", "Executions, by final status.");
        _runDuration = meter.CreateHistogram<double>("batch.run.duration", "s", "Duration of an execution.");
        _items = meter.CreateCounter<long>("batch.items", "{item}", "Items read, written or skipped.");
        _chunkDuration = meter.CreateHistogram<double>("batch.chunk.duration", "s", "Duration of a chunk write, retries included.");
    }

    public void RunCompleted(string job, JobStatus status, TimeSpan duration)
    {
        var tags = new TagList { { "batch.job", job }, { "batch.status", status.ToString() } };
        _runs.Add(1, tags);
        _runDuration.Record(duration.TotalSeconds, tags);
    }

    public void RunRejected(string job, string reason) =>
        _runs.Add(1, new TagList { { "batch.job", job }, { "batch.status", reason } });

    public void Items(string job, string outcome, long count)
    {
        if (count > 0)
        {
            _items.Add(count, new TagList { { "batch.job", job }, { "batch.item.outcome", outcome } });
        }
    }

    public void ChunkWritten(string job, TimeSpan duration) =>
        _chunkDuration.Record(duration.TotalSeconds, new TagList { { "batch.job", job } });
}
