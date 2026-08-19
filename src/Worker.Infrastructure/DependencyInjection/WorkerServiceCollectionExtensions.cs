using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Worker.Contracts;
using Worker.Core;
using Worker.Infrastructure;

namespace Worker.Infrastructure.DependencyInjection;

public static class WorkerServiceCollectionExtensions
{
    public static IServiceCollection AddWorkerPlatform(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<WorkerPlatformOptions>(configuration.GetSection("WorkerPlatform"));
        services.AddSingleton<IWorkerConfigurationRepository, DatabaseWorkerConfigurationRepository>();
        services.AddSingleton<IWorkerRegistry, WorkerRegistry>();
        services.AddSingleton<IWorkerEngine, WorkerEngine>();
        services.AddSingleton<IWorkerScheduler, CronWorkerScheduler>();

        services.AddSingleton<IWorkerDataSource, PostgresWorkerDataSource>();
        services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
        services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
        services.AddSingleton<IWorkerResultStore, PostgresWorkerResultStore>();
        services.AddSingleton<INotificationService, PostgresNotificationService>();

        return services;
    }

    public static IServiceCollection AddWorker<T>(this IServiceCollection services)
        where T : class, IWorkerHandler
    {
        services.AddSingleton<T>();
        services.AddSingleton<IWorkerHandler>(sp => sp.GetRequiredService<T>());
        return services;
    }
}
