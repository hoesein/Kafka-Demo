using Worker.Contracts;
using Worker.Infrastructure.DependencyInjection;
using Worker.Plugins;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWorkerPlatform(builder.Configuration);

var workerTypeRegistrations = new Dictionary<string, Action>(StringComparer.OrdinalIgnoreCase)
{
    ["DatabaseProducer"] = () => builder.Services.AddWorker<DatabaseProducerWorker>(),
    ["KafkaConsumer"] = () => builder.Services.AddWorker<KafkaConsumerWorker>(),
    ["Notification"] = () => builder.Services.AddWorker<NotificationWorker>()
};

var configuredWorkerTypes = Environment.GetEnvironmentVariable("WORKER_TYPE");
var workerTypes = string.IsNullOrWhiteSpace(configuredWorkerTypes)
    ? workerTypeRegistrations.Keys.ToArray()
    : configuredWorkerTypes
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();

foreach (var workerType in workerTypes)
{
    if (!workerTypeRegistrations.TryGetValue(workerType, out var registerWorker))
    {
        throw new InvalidOperationException($"Unsupported worker type '{workerType}'. Supported types: {string.Join(", ", workerTypeRegistrations.Keys)}");
    }

    registerWorker();
}

var host = builder.Build();
var registry = host.Services.GetRequiredService<IWorkerRegistry>();

foreach (var handler in host.Services.GetServices<IWorkerHandler>())
{
    registry.Register(handler);
}

var scheduler = host.Services.GetRequiredService<IWorkerScheduler>();
var startupLogger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Worker.Host");
await scheduler.StartAsync(CancellationToken.None);

try
{
    startupLogger.LogInformation("Starting worker services in parallel: {WorkerTypes}", string.Join(", ", workerTypes));
    await host.RunAsync();
}
finally
{
    await scheduler.StopAsync(CancellationToken.None);
}