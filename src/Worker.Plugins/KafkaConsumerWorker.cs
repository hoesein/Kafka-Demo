using Microsoft.Extensions.Logging;
using Worker.Contracts;

namespace Worker.Plugins;

public sealed class KafkaConsumerWorker : IWorkerHandler
{
    private readonly IKafkaConsumerService _consumerService;
    private readonly IWorkerResultStore _resultStore;
    private readonly ILogger<KafkaConsumerWorker> _logger;

    public KafkaConsumerWorker(
        IKafkaConsumerService consumerService,
        IWorkerResultStore resultStore,
        ILogger<KafkaConsumerWorker> logger)
    {
        _consumerService = consumerService;
        _resultStore = resultStore;
        _logger = logger;
    }

    public string WorkerType => "KafkaConsumer";

    public async Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var topic = context.Metadata.TryGetValue("Topic", out var configuredTopic) ? configuredTopic : string.Empty;
        var consumerGroup = context.Metadata.TryGetValue("ConsumerGroup", out var configuredGroup) ? configuredGroup : "default-consumer-group";
        var batchSize = context.Metadata.TryGetValue("BatchSize", out var batchValue) && int.TryParse(batchValue, out var parsedBatch) ? parsedBatch : 100;
        var connectionString = context.Metadata.TryGetValue("TargetConnectionString", out var configuredTargetConnectionString)
            ? configuredTargetConnectionString
            : context.Metadata.TryGetValue("TargetDatabaseConnectionString", out var configuredTargetDatabaseConnectionString)
                ? configuredTargetDatabaseConnectionString
                : context.Metadata.TryGetValue("ConnectionString", out var configuredConnectionString)
                    ? configuredConnectionString
                    : context.Metadata.TryGetValue("DatabaseConnectionString", out var configuredDatabaseConnectionString)
                        ? configuredDatabaseConnectionString
                        : null;

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new InvalidOperationException("KafkaConsumer worker requires a Topic in the worker metadata.");
        }

        _logger.LogInformation("Consuming Kafka messages from topic {Topic} for group {ConsumerGroup}.", topic, consumerGroup);

        var messages = await _consumerService.ConsumeAsync(topic, consumerGroup, batchSize, cancellationToken);
        if (messages.Count == 0)
        {
            _logger.LogInformation("No messages were consumed from topic {Topic}.", topic);
            return;
        }

        await _resultStore.StoreAsync(context.WorkerName, topic, messages, connectionString, cancellationToken);
        _logger.LogInformation("Stored {MessageCount} consumed messages for worker {WorkerName}.", messages.Count, context.WorkerName);
    }
}


























































































































































































































































































