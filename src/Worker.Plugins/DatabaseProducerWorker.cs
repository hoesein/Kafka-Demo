using Microsoft.Extensions.Logging;
using Worker.Contracts;

namespace Worker.Plugins;

public sealed class DatabaseProducerWorker : IWorkerHandler
{
    private readonly IWorkerDataSource _dataSource;
    private readonly IKafkaProducerService _kafkaProducerService;
    private readonly ILogger<DatabaseProducerWorker> _logger;

    public DatabaseProducerWorker(
        IWorkerDataSource dataSource,
        IKafkaProducerService kafkaProducerService,
        ILogger<DatabaseProducerWorker> logger)
    {
        _dataSource = dataSource;
        _kafkaProducerService = kafkaProducerService;
        _logger = logger;
    }

    public string WorkerType => "DatabaseProducer";

    public async Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var query = context.Metadata.TryGetValue("Query", out var configuredQuery) ? configuredQuery : string.Empty;
        var topic = context.Metadata.TryGetValue("KafkaTopic", out var configuredTopic) ? configuredTopic : "notification-topic";
        var batchSize = context.Metadata.TryGetValue("BatchSize", out var batchValue) && int.TryParse(batchValue, out var parsedBatch) ? parsedBatch : 100;
        var connectionString = context.Metadata.TryGetValue("SourceConnectionString", out var configuredSourceConnectionString)
            ? configuredSourceConnectionString
            : context.Metadata.TryGetValue("ConnectionString", out var configuredConnectionString)
                ? configuredConnectionString
                : context.Metadata.TryGetValue("DatabaseConnectionString", out var configuredDatabaseConnectionString)
                    ? configuredDatabaseConnectionString
                    : null;

        if (string.IsNullOrWhiteSpace(query))
        {
            throw new InvalidOperationException("DatabaseProducer worker requires a Query in the worker metadata.");
        }

        _logger.LogInformation("Fetching pending rows for worker {WorkerName} with query {Query}.", context.WorkerName, query);

        var messages = await _dataSource.GetPendingMessagesAsync(context.WorkerName, query, batchSize, connectionString, cancellationToken);
        if (messages.Count == 0)
        {
            _logger.LogInformation("No records returned for {WorkerName}.", context.WorkerName);
            return;
        }

        await _kafkaProducerService.PublishAsync(topic, messages, cancellationToken);
        _logger.LogInformation("Published {MessageCount} messages to topic {Topic}.", messages.Count, topic);
    }
}