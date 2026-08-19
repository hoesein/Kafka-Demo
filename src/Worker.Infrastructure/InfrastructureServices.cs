using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Worker.Contracts;

namespace Worker.Infrastructure;

public sealed class PostgresWorkerDataSource : IWorkerDataSource
{
    private readonly WorkerPlatformOptions _options;
    private readonly ILogger<PostgresWorkerDataSource> _logger;

    public PostgresWorkerDataSource(IOptions<WorkerPlatformOptions> options, ILogger<PostgresWorkerDataSource> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyCollection<string>> GetPendingMessagesAsync(string workerName, string query, int batchSize, string? connectionString, CancellationToken cancellationToken)
    {
        var effectiveConnectionString = ResolveConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(effectiveConnectionString))
        {
            _logger.LogWarning("Database connection string is empty for worker {WorkerName}. Returning no messages.", workerName);
            return Array.Empty<string>();
        }

        try
        {
            await using var connection = new NpgsqlConnection(effectiveConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = 30;

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var messages = new List<string>();

            while (await reader.ReadAsync(cancellationToken) && messages.Count < batchSize)
            {
                if (reader[0] is not null)
                {
                    var value = reader.GetValue(0)?.ToString();
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        messages.Add(value);
                    }
                }
            }

            return messages;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read pending messages from PostgreSQL for worker {WorkerName}.", workerName);
            return Array.Empty<string>();
        }
    }

    private string? ResolveConnectionString(string? configuredConnectionString)
    {
        if (!string.IsNullOrWhiteSpace(configuredConnectionString))
        {
            return configuredConnectionString;
        }

        return _options.Database.DefaultConnectionString;
    }
}

public sealed class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IOptions<WorkerPlatformOptions> options, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.Kafka.BootstrapServers,
            MessageTimeoutMs = 60000,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task PublishAsync(string topic, IReadOnlyCollection<string> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var delivery = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message }, cancellationToken);
            _logger.LogInformation("Published message to Kafka topic {Topic} with offset {Offset}.", topic, delivery.Offset.Value);
        }
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}

public sealed class KafkaConsumerService : IKafkaConsumerService, IDisposable
{
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(IOptions<WorkerPlatformOptions> options, ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.Kafka.BootstrapServers,
            GroupId = "worker-platform-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
    }

    public Task<IReadOnlyCollection<string>> ConsumeAsync(string topic, string consumerGroup, int batchSize, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);

        var items = new List<string>();
        var timeout = TimeSpan.FromSeconds(5);

        while (items.Count < batchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var result = _consumer.Consume(timeout);
            if (result is null)
            {
                break;
            }

            if (!string.IsNullOrWhiteSpace(result.Message.Value))
            {
                items.Add(result.Message.Value);
            }

            _consumer.StoreOffset(result);
        }

        _consumer.Commit();
        _logger.LogInformation("Consumed {Count} message(s) from Kafka topic {Topic}.", items.Count, topic);
        return Task.FromResult<IReadOnlyCollection<string>>(items);
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }
}

public sealed class PostgresWorkerResultStore : IWorkerResultStore
{
    private readonly WorkerPlatformOptions _options;
    private readonly ILogger<PostgresWorkerResultStore> _logger;

    public PostgresWorkerResultStore(IOptions<WorkerPlatformOptions> options, ILogger<PostgresWorkerResultStore> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task StoreAsync(string workerName, string topic, IReadOnlyCollection<string> messages, string? connectionString, CancellationToken cancellationToken)
    {
        var effectiveConnectionString = ResolveConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(effectiveConnectionString))
        {
            _logger.LogInformation("No database configured. Skipping persistence of Kafka consumer results for worker {WorkerName}.", workerName);
            return;
        }

        try
        {
            await using var connection = new NpgsqlConnection(effectiveConnectionString);
            await connection.OpenAsync(cancellationToken);

            foreach (var message in messages)
            {
                var payload = JsonSerializer.Serialize(new
                {
                    workerName,
                    topic,
                    message,
                    storedAtUtc = DateTimeOffset.UtcNow
                });

                await using var insertQueue = new NpgsqlCommand(
                    @"INSERT INTO notification_queue (message_key, payload, status) VALUES (@messageKey, @payload, 'NEW')",
                    connection);

                insertQueue.Parameters.AddWithValue("messageKey", $"{workerName}-{Guid.NewGuid():N}");
                insertQueue.Parameters.AddWithValue("payload", payload);
                await insertQueue.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist Kafka consumer results for worker {WorkerName}.", workerName);
        }
    }

    private string? ResolveConnectionString(string? configuredConnectionString)
    {
        if (!string.IsNullOrWhiteSpace(configuredConnectionString))
        {
            return configuredConnectionString;
        }

        return _options.Database.DefaultConnectionString;
    }
}

public sealed class PostgresNotificationService : INotificationService
{
    private readonly WorkerPlatformOptions _options;
    private readonly ILogger<PostgresNotificationService> _logger;

    public PostgresNotificationService(IOptions<WorkerPlatformOptions> options, ILogger<PostgresNotificationService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task SendAsync(string channel, string recipient, string message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Dispatching notification via {Channel} to {Recipient}. Message: {Message}", channel, recipient, message);

        if (string.IsNullOrWhiteSpace(_options.Database.DefaultConnectionString))
        {
            return;
        }

        try
        {
            await using var connection = new NpgsqlConnection(_options.Database.DefaultConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new NpgsqlCommand(
                @"INSERT INTO notification_queue (message_key, payload, status)
                  VALUES (@messageKey, @payload, 'NEW')",
                connection);

            var payload = JsonSerializer.Serialize(new
            {
                channel,
                recipient,
                message,
                sentAtUtc = DateTimeOffset.UtcNow
            });

            command.Parameters.AddWithValue("messageKey", $"notification-{Guid.NewGuid():N}");
            command.Parameters.AddWithValue("payload", payload);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist notification for recipient {Recipient}.", recipient);
            throw;
        }
    }
}
