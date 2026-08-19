namespace Worker.Contracts;

public interface IWorkerDataSource
{
    Task<IReadOnlyCollection<string>> GetPendingMessagesAsync(string workerName, string query, int batchSize, string? connectionString, CancellationToken cancellationToken);
}

public interface IKafkaProducerService
{
    Task PublishAsync(string topic, IReadOnlyCollection<string> messages, CancellationToken cancellationToken);
}

public interface IKafkaConsumerService
{
    Task<IReadOnlyCollection<string>> ConsumeAsync(string topic, string consumerGroup, int batchSize, CancellationToken cancellationToken);
}

public interface IWorkerResultStore
{
    Task StoreAsync(string workerName, string topic, IReadOnlyCollection<string> messages, string? connectionString, CancellationToken cancellationToken);
}

public interface INotificationService
{
    Task SendAsync(string channel, string recipient, string message, CancellationToken cancellationToken);
}

public interface IWorkerExecutionHistoryStore
{
    void RecordStart(string workerName, string workerType, string correlationId, DateTimeOffset startedAt);

    void RecordCompletion(string workerName, string workerType, string correlationId, bool succeeded, int processedRecordCount, string? errorMessage, DateTimeOffset startedAt, DateTimeOffset finishedAt);

    IReadOnlyCollection<WorkerExecutionRecord> GetHistory();
}

public sealed class WorkerExecutionRecord
{
    public string WorkerName { get; init; } = string.Empty;

    public string WorkerType { get; init; } = string.Empty;

    public string CorrelationId { get; init; } = string.Empty;

    public DateTimeOffset StartedAt { get; init; }

    public DateTimeOffset FinishedAt { get; init; }

    public bool Succeeded { get; init; }

    public int ProcessedRecordCount { get; init; }

    public string? ErrorMessage { get; init; }
}
