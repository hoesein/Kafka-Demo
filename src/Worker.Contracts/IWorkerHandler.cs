namespace Worker.Contracts;

public interface IWorkerHandler
{
    string WorkerType { get; }

    Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken);
}
