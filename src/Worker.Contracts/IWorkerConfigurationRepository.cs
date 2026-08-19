namespace Worker.Contracts;

public interface IWorkerConfigurationRepository
{
    Task<IReadOnlyCollection<WorkerDefinition>> GetActiveWorkersAsync(CancellationToken cancellationToken = default);

    Task<WorkerDefinition?> GetByNameAsync(string workerName, CancellationToken cancellationToken = default);

    Task SaveAsync(WorkerDefinition definition, CancellationToken cancellationToken = default);
}
