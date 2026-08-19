using Worker.Contracts;

namespace Worker.Infrastructure;

public sealed class DatabaseWorkerConfigurationRepository : IWorkerConfigurationRepository
{
    private readonly List<WorkerDefinition> _workers;

    public DatabaseWorkerConfigurationRepository()
    {
        _workers = new List<WorkerDefinition>
        {
            new()
            {
                Id = 1,
                Name = "DatabaseProducer",
                WorkerType = "DatabaseProducer",
                CronExpression = "*/5 * * * *",
                IsEnabled = true,
                RetryCount = 3,
                BatchSize = 250,
                TimeoutSeconds = 120,
                ConfigurationJson = "{\"Query\":\"select * from notification_queue where status = 'NEW'\",\"KafkaTopic\":\"notification-topic\",\"SourceConnectionString\":\"Host=db-one;Port=5432;Database=db_one;Username=worker;Password=workerpass;\"}",
                CreatedDate = DateTimeOffset.UtcNow,
                UpdatedDate = DateTimeOffset.UtcNow
            },
            new()
            {
                Id = 2,
                Name = "KafkaConsumer",
                WorkerType = "KafkaConsumer",
                CronExpression = "*/2 * * * *",
                IsEnabled = true,
                RetryCount = 2,
                BatchSize = 100,
                TimeoutSeconds = 90,
                ConfigurationJson = "{\"Topic\":\"sms-topic\",\"ConsumerGroup\":\"sms-service\",\"TargetConnectionString\":\"Host=db-two;Port=5432;Database=db_two;Username=worker;Password=workerpass;\"}",
                CreatedDate = DateTimeOffset.UtcNow,
                UpdatedDate = DateTimeOffset.UtcNow
            },
            new()
            {
                Id = 3,
                Name = "Notification",
                WorkerType = "Notification",
                CronExpression = "*/10 * * * *",
                IsEnabled = true,
                RetryCount = 3,
                BatchSize = 50,
                TimeoutSeconds = 60,
                ConfigurationJson = "{\"Channel\":\"email\",\"Recipient\":\"ops@example.com\",\"Message\":\"Scheduled worker notification\"}",
                CreatedDate = DateTimeOffset.UtcNow,
                UpdatedDate = DateTimeOffset.UtcNow
            }
        };

        foreach (var worker in _workers)
        {
            worker.ApplyConfigurationJson();
        }
    }

    public Task<IReadOnlyCollection<WorkerDefinition>> GetActiveWorkersAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult<IReadOnlyCollection<WorkerDefinition>>(
            _workers
                .Where(w => w.IsEnabled)
                .Select(Clone)
                .ToArray());
    }

    public Task<WorkerDefinition?> GetByNameAsync(string workerName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var worker = _workers.FirstOrDefault(w => string.Equals(w.Name, workerName, StringComparison.OrdinalIgnoreCase));
        return Task.FromResult(worker is null ? null : Clone(worker));
    }

    public Task SaveAsync(WorkerDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        cancellationToken.ThrowIfCancellationRequested();

        definition.ApplyConfigurationJson();

        var existingIndex = _workers.FindIndex(w => w.Id == definition.Id || string.Equals(w.Name, definition.Name, StringComparison.OrdinalIgnoreCase));
        if (existingIndex >= 0)
        {
            _workers[existingIndex] = Clone(definition);
            _workers[existingIndex].UpdatedDate = DateTimeOffset.UtcNow;
        }
        else
        {
            definition.Id = _workers.Count == 0 ? 1 : _workers.Max(w => w.Id) + 1;
            definition.CreatedDate = DateTimeOffset.UtcNow;
            definition.UpdatedDate = DateTimeOffset.UtcNow;
            _workers.Add(Clone(definition));
        }

        return Task.CompletedTask;
    }

    private static WorkerDefinition Clone(WorkerDefinition source)
    {
        return new WorkerDefinition
        {
            Id = source.Id,
            Name = source.Name,
            WorkerType = source.WorkerType,
            CronExpression = source.CronExpression,
            IsEnabled = source.IsEnabled,
            RetryCount = source.RetryCount,
            TimeoutSeconds = source.TimeoutSeconds,
            BatchSize = source.BatchSize,
            ConfigurationJson = source.ConfigurationJson,
            Settings = new Dictionary<string, string>(source.Settings, StringComparer.OrdinalIgnoreCase),
            CreatedDate = source.CreatedDate,
            UpdatedDate = source.UpdatedDate
        };
    }
}
