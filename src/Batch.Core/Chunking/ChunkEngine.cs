using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Batch.Abstractions;
using Batch.Core.Execution;
using Batch.Core.Resilience;
using Batch.Core.Telemetry;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Registry;

namespace Batch.Core.Chunking;

/// <summary>
/// Pipeline à trois étages, reliés par des canaux bornés :
/// <code>
///   Lecture ──[chunks]──▶ Traitement (parallèle borné) ──[chunks]──▶ Écriture (séquentielle) ──▶ Checkpoint
/// </code>
/// Les étages se chevauchent : on lit le chunk N+2 pendant qu'on traite N+1 et qu'on écrit N.
/// La capacité bornée des canaux crée la contre-pression : une écriture lente ralentit la lecture,
/// la mémoire reste constante quel que soit le volume.
/// </summary>
internal sealed class ChunkEngine(ResiliencePipelineProvider<string> pipelines, BatchMetrics metrics, TimeProvider time)
{
    public async Task<JobResult> RunAsync<TParams, TIn, TOut>(
        IChunkedJob<TParams, TIn, TOut> job,
        JobContext<TParams> context,
        RunInvocation invocation,
        CancellationToken cancellationToken)
        where TParams : class, new()
        where TIn : notnull
        where TOut : class
    {
        var run = new ChunkRun<TParams, TIn, TOut>(
            job,
            context,
            invocation,
            pipelines.GetPipeline(BatchResilience.PipelineName),
            metrics,
            time);

        await run.ExecuteAsync(cancellationToken);

        // Le statut « avec avertissements » se décide dans JobRunner, pour tous les types de jobs.
        var counters = context.Progress.Snapshot();
        return JobResult.Success($"Read {counters.Read}, written {counters.Written}, skipped {counters.Skipped}.");
    }
}

internal sealed record ReadChunk<TIn>(long Sequence, List<TIn> Items);

internal sealed record ProcessedChunk<TOut>(long Sequence, List<TOut> Items, string Checkpoint);

internal sealed class ChunkRun<TParams, TIn, TOut>
    where TParams : class, new()
    where TIn : notnull
    where TOut : class
{
    private readonly IChunkedJob<TParams, TIn, TOut> _job;
    private readonly JobContext<TParams> _context;
    private readonly RunInvocation _invocation;
    private readonly ResiliencePipeline _pipeline;
    private readonly BatchMetrics _metrics;
    private readonly TimeProvider _time;
    private readonly BatchJobOptions _options;
    private readonly SkipBudget _skips;

    public ChunkRun(
        IChunkedJob<TParams, TIn, TOut> job,
        JobContext<TParams> context,
        RunInvocation invocation,
        ResiliencePipeline pipeline,
        BatchMetrics metrics,
        TimeProvider time)
    {
        _job = job;
        _context = context;
        _invocation = invocation;
        _pipeline = pipeline;
        _metrics = metrics;
        _time = time;
        _options = invocation.Options;
        _skips = new SkipBudget(invocation.Options.SkipLimit);
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = cts.Token;

        var bounded = new BoundedChannelOptions(_options.PipelineDepth)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        };
        var toProcess = Channel.CreateBounded<ReadChunk<TIn>>(bounded);
        var toWrite = Channel.CreateBounded<ProcessedChunk<TOut>>(bounded);

        Task[] stages =
        [
            Guard(ReadStageAsync(toProcess.Writer, token), cts),
            Guard(ProcessStageAsync(toProcess.Reader, toWrite.Writer, token), cts),
            Guard(WriteStageAsync(toWrite.Reader, token), cts),
        ];

        try
        {
            await Task.WhenAll(stages);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // Quand un étage échoue, les autres sont annulés et lèvent OperationCanceledException.
            // On remonte la cause racine, pas l'annulation qu'elle a provoquée.
            var root = stages
                .Where(t => t.IsFaulted)
                .SelectMany(t => t.Exception!.InnerExceptions)
                .FirstOrDefault(e => e is not OperationCanceledException);

            if (root is not null)
            {
                ExceptionDispatchInfo.Throw(root);
            }

            throw;
        }
    }

    private async Task ReadStageAsync(ChannelWriter<ReadChunk<TIn>> output, CancellationToken cancellationToken)
    {
        try
        {
            long sequence = 0;
            var buffer = new List<TIn>(_options.ChunkSize);

            await foreach (var item in _job.ReadAsync(_context, _invocation.ResumeCheckpoint, cancellationToken).WithCancellation(cancellationToken))
            {
                buffer.Add(item);
                if (buffer.Count < _options.ChunkSize)
                {
                    continue;
                }

                await EmitAsync(buffer);
                buffer = new List<TIn>(_options.ChunkSize);
            }

            if (buffer.Count > 0)
            {
                await EmitAsync(buffer);
            }

            output.TryComplete();

            async ValueTask EmitAsync(List<TIn> items)
            {
                _context.Progress.AddRead(items.Count);
                _metrics.Items(_context.JobName, "read", items.Count);
                await output.WriteAsync(new ReadChunk<TIn>(sequence++, items), cancellationToken);
            }
        }
        catch (Exception ex)
        {
            output.TryComplete(ex);
            throw;
        }
    }

    private async Task ProcessStageAsync(
        ChannelReader<ReadChunk<TIn>> input,
        ChannelWriter<ProcessedChunk<TOut>> output,
        CancellationToken cancellationToken)
    {
        try
        {
            var parallel = new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism,
                CancellationToken = cancellationToken,
            };

            await foreach (var chunk in input.ReadAllAsync(cancellationToken))
            {
                var results = new TOut?[chunk.Items.Count];

                if (_options.MaxDegreeOfParallelism == 1)
                {
                    for (var i = 0; i < chunk.Items.Count; i++)
                    {
                        results[i] = await ProcessItemAsync(chunk.Items[i], cancellationToken);
                    }
                }
                else
                {
                    // Chaque résultat garde l'index de son item : l'ordre de sortie est celui de lecture.
                    await Parallel.ForEachAsync(
                        Enumerable.Range(0, chunk.Items.Count),
                        parallel,
                        async (i, ct) => results[i] = await ProcessItemAsync(chunk.Items[i], ct));
                }

                var outputs = new List<TOut>(results.Length);
                foreach (var result in results)
                {
                    if (result is not null)
                    {
                        outputs.Add(result);
                    }
                }

                // Le checkpoint est celui du dernier item LU du chunk, filtré ou rejeté compris.
                var checkpoint = _job.CheckpointOf(chunk.Items[^1]);
                await output.WriteAsync(new ProcessedChunk<TOut>(chunk.Sequence, outputs, checkpoint), cancellationToken);
            }

            output.TryComplete();
        }
        catch (Exception ex)
        {
            output.TryComplete(ex);
            throw;
        }
    }

    private async ValueTask<TOut?> ProcessItemAsync(TIn item, CancellationToken cancellationToken)
    {
        try
        {
            return await _pipeline.ExecuteAsync(ct => _job.ProcessAsync(item, _context, ct), cancellationToken);
        }
        catch (SkippableItemException ex)
        {
            _skips.Register();
            _context.Progress.AddSkipped();
            _metrics.Items(_context.JobName, "skipped", 1);
            ChunkLog.ItemSkipped(_context.Logger, _job.CheckpointOf(item), ex.Message);
            return null;
        }
    }

    private async Task WriteStageAsync(ChannelReader<ProcessedChunk<TOut>> input, CancellationToken cancellationToken)
    {
        await foreach (var chunk in input.ReadAllAsync(cancellationToken))
        {
            var startedAt = _time.GetTimestamp();

            using (var activity = BatchTelemetry.ActivitySource.StartActivity("batch.chunk.write"))
            {
                activity?.SetTag("batch.chunk.sequence", chunk.Sequence);
                activity?.SetTag("batch.chunk.size", chunk.Items.Count);

                if (chunk.Items.Count > 0)
                {
                    await _pipeline.ExecuteAsync(
                        ct => new ValueTask(_job.WriteAsync(chunk.Items, _context, ct)),
                        cancellationToken);
                }
            }

            _context.Progress.AddWritten(chunk.Items.Count);
            _metrics.Items(_context.JobName, "written", chunk.Items.Count);

            // Le chunk est écrit : on enregistre la position même si un arrêt vient d'être demandé,
            // sinon le redémarrage réécrirait inutilement ce chunk.
            await _invocation.SaveCheckpointAsync(chunk.Checkpoint, CancellationToken.None);

            _metrics.ChunkWritten(_context.JobName, _time.GetElapsedTime(startedAt));
            ChunkLog.ChunkCommitted(_context.Logger, chunk.Sequence, chunk.Items.Count, chunk.Checkpoint);
        }
    }

    private static async Task Guard(Task stage, CancellationTokenSource cts)
    {
        try
        {
            await stage;
        }
        catch
        {
            await cts.CancelAsync();
            throw;
        }
    }
}

/// <summary>Compteur de rejets thread-safe : lève dès que le seuil est franchi.</summary>
internal sealed class SkipBudget(int limit)
{
    private long _count;

    public long Count => Interlocked.Read(ref _count);

    public void Register()
    {
        var count = Interlocked.Increment(ref _count);
        if (count > limit)
        {
            throw new SkipLimitExceededException(count, limit);
        }
    }
}

internal static partial class ChunkLog
{
    [LoggerMessage(2000, LogLevel.Warning, "Item at {Position} skipped: {Reason}")]
    public static partial void ItemSkipped(ILogger logger, string position, string reason);

    [LoggerMessage(2001, LogLevel.Debug, "Chunk #{Sequence} committed: {Count} item(s), checkpoint {Checkpoint}.")]
    public static partial void ChunkCommitted(ILogger logger, long sequence, int count, string checkpoint);
}
