using Worker.Contracts;

namespace Worker.Api;

public sealed class InMemoryWorkerExecutionHistoryStore : IWorkerExecutionHistoryStore
{
    private readonly List<WorkerExecutionRecord> _history = new();
    private readonly object _lock = new();

    public void RecordStart(string workerName, string workerType, string correlationId, DateTimeOffset startedAt)
    {
        lock (_lock)
        {
            _history.Add(new WorkerExecutionRecord
            {
                WorkerName = workerName,
                WorkerType = workerType,
                CorrelationId = correlationId,
                StartedAt = startedAt,
                FinishedAt = startedAt,
                Succeeded = false,
                ProcessedRecordCount = 0,
                ErrorMessage = null
            });
        }
    }

    public void RecordCompletion(string workerName, string workerType, string correlationId, bool succeeded, int processedRecordCount, string? errorMessage, DateTimeOffset startedAt, DateTimeOffset finishedAt)
    {
        lock (_lock)
        {
            var current = _history.LastOrDefault(x =>
                string.Equals(x.WorkerName, workerName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.CorrelationId, correlationId, StringComparison.OrdinalIgnoreCase));

            if (current is not null)
            {
                var index = _history.IndexOf(current);
                _history[index] = new WorkerExecutionRecord
                {
                    WorkerName = current.WorkerName,
                    WorkerType = current.WorkerType,
                    CorrelationId = current.CorrelationId,
                    StartedAt = startedAt,
                    FinishedAt = finishedAt,
                    Succeeded = succeeded,
                    ProcessedRecordCount = processedRecordCount,
                    ErrorMessage = errorMessage
                };
                return;
            }

            _history.Add(new WorkerExecutionRecord
            {
                WorkerName = workerName,
                WorkerType = workerType,
                CorrelationId = correlationId,
                StartedAt = startedAt,
                FinishedAt = finishedAt,
                Succeeded = succeeded,
                ProcessedRecordCount = processedRecordCount,
                ErrorMessage = errorMessage
            });
        }
    }

    public IReadOnlyCollection<WorkerExecutionRecord> GetHistory()
    {
        lock (_lock)
        {
            return _history
                .OrderByDescending(x => x.FinishedAt)
                .ToArray();
        }
    }
}
