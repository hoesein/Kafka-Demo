namespace Worker.Contracts;

public sealed class WorkerContext
{
    public string WorkerName { get; init; } = string.Empty;

    public string WorkerType { get; init; } = string.Empty;

    public string CorrelationId { get; init; } = Guid.NewGuid().ToString("N");

    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;

    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
}
