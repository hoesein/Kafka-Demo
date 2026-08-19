using Microsoft.Extensions.Logging;
using Worker.Contracts;

namespace Worker.Core;

public sealed class CronWorkerScheduler : IWorkerScheduler
{
    private readonly IWorkerEngine _engine;
    private readonly IWorkerConfigurationRepository _configurationRepository;
    private readonly ILogger<CronWorkerScheduler> _logger;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _backgroundTask;
    private IReadOnlyCollection<WorkerDefinition> _definitions = Array.Empty<WorkerDefinition>();

    public CronWorkerScheduler(
        IWorkerEngine engine,
        IWorkerConfigurationRepository configurationRepository,
        ILogger<CronWorkerScheduler> logger)
    {
        _engine = engine;
        _configurationRepository = configurationRepository;
        _logger = logger;
    }

    public IReadOnlyCollection<WorkerDefinition> GetDefinitions()
    {
        return _definitions;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _definitions = await _configurationRepository.GetActiveWorkersAsync(cancellationToken);
        _backgroundTask = Task.Run(() => RunLoopAsync(cancellationToken), cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _shutdown.Cancel();

        if (_backgroundTask is not null)
        {
            await _backgroundTask.WaitAsync(cancellationToken);
        }
    }

    public async Task RunDueWorkersAsync(CancellationToken cancellationToken)
    {
        _definitions = await _configurationRepository.GetActiveWorkersAsync(cancellationToken);
        await _engine.ExecuteDueWorkersAsync(cancellationToken);
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Cron scheduler started.");

        while (!cancellationToken.IsCancellationRequested && !_shutdown.IsCancellationRequested)
        {
            await RunDueWorkersAsync(cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(15), cancellationToken);
        }
    }
}






















































































































































































































































































