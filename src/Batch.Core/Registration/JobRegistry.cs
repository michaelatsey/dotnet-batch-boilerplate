using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Batch.Core.Registration;

public interface IJobRegistry
{
    IReadOnlyCollection<JobDescriptor> Jobs { get; }

    bool TryGet(string name, [NotNullWhen(true)] out JobDescriptor? descriptor);
}

internal sealed class JobRegistry : IJobRegistry
{
    private readonly FrozenDictionary<string, JobDescriptor> _jobs;

    public JobRegistry(IEnumerable<JobDescriptor> descriptors)
    {
        var all = descriptors.ToList();
        var duplicate = all
            .GroupBy(d => d.Name, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(g => g.Count() > 1);

        if (duplicate is not null)
        {
            throw new InvalidOperationException($"Job '{duplicate.Key}' is registered more than once.");
        }

        _jobs = all.ToFrozenDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyCollection<JobDescriptor> Jobs => _jobs.Values;

    public bool TryGet(string name, [NotNullWhen(true)] out JobDescriptor? descriptor) =>
        _jobs.TryGetValue(name, out descriptor);
}
