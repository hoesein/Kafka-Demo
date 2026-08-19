namespace Worker.Contracts;

public interface IWorkerScheduler
{
    IReadOnlyCollection<WorkerDefinition> GetDefinitions();

    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);

    Task RunDueWorkersAsync(CancellationToken cancellationToken);
}
