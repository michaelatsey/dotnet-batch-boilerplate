using System.Data.Common;
using Batch.Abstractions;
using Batch.Core.Resilience;

namespace Batch.Core.Tests;

public sealed class TransientErrorClassifierTests
{
    private static readonly DefaultTransientErrorClassifier Classifier = new();

    public static TheoryData<Exception, bool> Cases => new()
    {
        // Timeout HttpClient : TaskCanceledException dont la cause est un TimeoutException. À retenter.
        { new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout", new TimeoutException()), true },
        { new TimeoutException(), true },
        { new HttpRequestException("connection reset"), true },
        { new IOException("broken pipe"), true },
        { new InvalidOperationException("wrapper", new TimeoutException()), true },

        // Annulation réelle (arrêt, timeout du job) : jamais retentée.
        { new OperationCanceledException(), false },
        { new TaskCanceledException(), false },
        { new BatchFunctionalException("bad data"), false },
        { new SkippableItemException("bad line"), false },
        { new InvalidOperationException("bug"), false },
        { new FakeDbException(transient: true), true },
        { new FakeDbException(transient: false), false },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Classifies_errors(Exception exception, bool expected) =>
        Assert.Equal(expected, Classifier.IsTransient(exception));

    private sealed class FakeDbException(bool transient) : DbException("db")
    {
        public override bool IsTransient => transient;
    }
}
