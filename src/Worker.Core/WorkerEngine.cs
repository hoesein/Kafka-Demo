using System.Collections.Concurrent;
using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Worker.Contracts;

namespace Worker.Core;

public sealed class WorkerEngine : BackgroundService, IWorkerEngine
{
    private readonly IWorkerRegistry _registry;
    private readonly IWorkerConfigurationRepository _configurationRepository;
    private readonly IWorkerExecutionHistoryStore? _historyStore;
    private readonly ILogger<WorkerEngine> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly HashSet<string> _inFlightWorkers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastRunUtc = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

    public WorkerEngine(
        IWorkerRegistry registry,
        IWorkerConfigurationRepository configurationRepository,
        ILogger<WorkerEngine> logger,
        IWorkerExecutionHistoryStore? historyStore = null,
        TimeProvider? timeProvider = null)
    {
        _registry = registry;
        _configurationRepository = configurationRepository;
        _historyStore = historyStore;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task ExecuteAsync(WorkerDefinition definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (!_registry.TryGet(definition.WorkerType, out var handler))
        {
            throw new InvalidOperationException($"No worker handler registered for type '{definition.WorkerType}'.");
        }

        var startedAt = DateTimeOffset.UtcNow;
        var correlationId = Guid.NewGuid().ToString("N");
        _historyStore?.RecordStart(definition.WorkerName, definition.WorkerType, correlationId, startedAt);

        var context = new WorkerContext
        {
            WorkerName = definition.WorkerName,
            WorkerType = definition.WorkerType,
            Metadata = definition.Settings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
            CorrelationId = correlationId,
            StartedAt = startedAt
        };

        _logger.LogInformation(
            "Starting worker {WorkerName} ({WorkerType}) with retry count {RetryCount}.",
            definition.WorkerName,
            definition.WorkerType,
            definition.RetryCount);

        var lastException = default(Exception);

        for (var attempt = 1; attempt <= definition.RetryCount; attempt++)
        {
            try
            {
                await handler.ExecuteAsync(context, cancellationToken);
                var finishedAt = DateTimeOffset.UtcNow;
                _lastRunUtc[definition.WorkerName] = finishedAt;
                _historyStore?.RecordCompletion(definition.WorkerName, definition.WorkerType, correlationId, true, 1, null, startedAt, finishedAt);
                _logger.LogInformation("Worker {WorkerName} completed successfully.", definition.WorkerName);
                return;
            }
            catch (Exception ex) when (attempt < definition.RetryCount)
            {
                lastException = ex;
                _logger.LogWarning(
                    ex,
                    "Worker {WorkerName} failed on attempt {Attempt}. Retrying.",
                    definition.WorkerName,
                    attempt);
            }
            catch (Exception ex)
            {
                lastException = ex;
                break;
            }
        }

        var failedAt = DateTimeOffset.UtcNow;
        _lastRunUtc[definition.WorkerName] = failedAt;
        _historyStore?.RecordCompletion(definition.WorkerName, definition.WorkerType, correlationId, false, 0, lastException?.Message ?? "Worker execution failed.", startedAt, failedAt);
        _logger.LogError(lastException, "Worker {WorkerName} reached the retry limit and failed.", definition.WorkerName);
    }

    public async Task ExecuteDueWorkersAsync(CancellationToken cancellationToken)
    {
        var definitions = await _configurationRepository.GetActiveWorkersAsync(cancellationToken);
        var now = _timeProvider.GetUtcNow();

        foreach (var definition in definitions)
        {
            if (!_registry.TryGet(definition.WorkerType, out _))
            {
                _logger.LogDebug(
                    "Skipping worker {WorkerName} ({WorkerType}) because this host does not register that handler type.",
                    definition.WorkerName,
                    definition.WorkerType);
                continue;
            }

            if (!IsDueNow(definition, now.UtcDateTime))
            {
                continue;
            }

            if (!TryAcquire(definition.WorkerName))
            {
                _logger.LogDebug("Skip worker {WorkerName} because it is already running.", definition.WorkerName);
                continue;
            }

            try
            {
                await ExecuteAsync(definition, cancellationToken);
            }
            finally
            {
                Release(definition.WorkerName);
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ExecuteDueWorkersAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);
        }
    }

    private bool IsDueNow(WorkerDefinition definition, DateTime utcNow)
    {
        if (string.IsNullOrWhiteSpace(definition.CronExpression))
        {
            return false;
        }

        try
        {
            var cron = CronExpression.Parse(definition.CronExpression);
            var tolerance = TimeSpan.FromSeconds(15);
            var baseTime = _lastRunUtc.TryGetValue(definition.WorkerName, out var lastRun)
                ? lastRun.UtcDateTime
                : utcNow.Subtract(tolerance);

            var next = cron.GetNextOccurrence(baseTime, TimeZoneInfo.Utc);
            if (next is null)
            {
                return false;
            }

            return next.Value >= utcNow.Subtract(tolerance) && next.Value <= utcNow;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Invalid cron expression '{CronExpression}' for worker {WorkerName}.", definition.CronExpression, definition.WorkerName);
            return false;
        }
    }

    private bool TryAcquire(string workerName)
    {
        lock (_lock)
        {
            if (_inFlightWorkers.Contains(workerName))
            {
                return false;
            }

            _inFlightWorkers.Add(workerName);
            return true;
        }
    }

    private void Release(string workerName)
    {
        lock (_lock)
        {
            _inFlightWorkers.Remove(workerName);
        }
    }
}
































































































































































































