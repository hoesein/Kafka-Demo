using Worker.Contracts;

namespace Worker.Core;

public sealed class WorkerRegistry : IWorkerRegistry
{
    private readonly Dictionary<string, IWorkerHandler> _handlers = new(StringComparer.OrdinalIgnoreCase);

    public void Register(IWorkerHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        _handlers[handler.WorkerType] = handler;
    }

    public bool TryGet(string workerType, out IWorkerHandler handler)
    {
        return _handlers.TryGetValue(workerType, out handler!);
    }

    public IReadOnlyCollection<IWorkerHandler> GetHandlers()
    {
        return _handlers.Values.ToArray();
    }
}
