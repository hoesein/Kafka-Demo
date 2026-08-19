namespace Worker.Contracts;

public interface IWorkerRegistry
{
    void Register(IWorkerHandler handler);

    bool TryGet(string workerType, out IWorkerHandler handler);

    IReadOnlyCollection<IWorkerHandler> GetHandlers();
}
