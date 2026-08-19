using Worker.Contracts;
using Worker.Infrastructure.DependencyInjection;
using Worker.Plugins;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWorkerPlatform(builder.Configuration);

var configuredWorkerTypes = Environment.GetEnvironmentVariable("WORKER_TYPE");
var workerTypes = string.IsNullOrWhiteSpace(configuredWorkerTypes)
    ? new[] { "DatabaseProducer", "KafkaConsumer", "Notification" }
    : configuredWorkerTypes
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();

if (workerTypes.Contains("DatabaseProducer", StringComparer.OrdinalIgnoreCase))
{
    builder.Services.AddWorker<DatabaseProducerWorker>();
}

if (workerTypes.Contains("KafkaConsumer", StringComparer.OrdinalIgnoreCase))
{
    builder.Services.AddWorker<KafkaConsumerWorker>();
}

if (workerTypes.Contains("Notification", StringComparer.OrdinalIgnoreCase))
{
    builder.Services.AddWorker<NotificationWorker>();
}

var host = builder.Build();
var registry = host.Services.GetRequiredService<IWorkerRegistry>();

foreach (var handler in host.Services.GetServices<IWorkerHandler>())
{
    registry.Register(handler);
}

var scheduler = host.Services.GetRequiredService<IWorkerScheduler>();
await scheduler.StartAsync(CancellationToken.None);

try
{
    await host.RunAsync();
}
finally
{
    await scheduler.StopAsync(CancellationToken.None);
}
























































