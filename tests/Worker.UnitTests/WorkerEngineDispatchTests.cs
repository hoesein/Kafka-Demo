using Microsoft.Extensions.Logging;
using NSubstitute;
using Worker.Contracts;
using Worker.Core;
using Xunit;

namespace Worker.UnitTests;

public sealed class WorkerEngineDispatchTests
{
    [Fact]
    public async Task ExecuteDueWorkersAsync_ShouldDispatchEveryMinuteWorker_OnFirstRun()
    {
        var definition = CreateDefinition("database-producer", "DatabaseProducer", "*/1 * * * *");
        var repository = Substitute.For<IWorkerConfigurationRepository>();
        repository.GetActiveWorkersAsync(Arg.Any<CancellationToken>())
            .Returns(new[] { definition });

        var handler = Substitute.For<IWorkerHandler>();
        handler.WorkerType.Returns("DatabaseProducer");
        var registry = new WorkerRegistry();
        registry.Register(handler);

        var engine = new WorkerEngine(
            registry,
            repository,
            Substitute.For<ILogger<WorkerEngine>>(),
            timeProvider: new FixedTimeProvider(new DateTimeOffset(2026, 1, 1, 12, 0, 5, TimeSpan.Zero)));

        await engine.ExecuteDueWorkersAsync(CancellationToken.None);

        await handler.Received(1).ExecuteAsync(
            Arg.Is<WorkerContext>(context => context.WorkerName == "database-producer"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteDueWorkersAsync_ShouldSkipForeignDefinition_AndDispatchRegisteredDefinition()
    {
        var repository = Substitute.For<IWorkerConfigurationRepository>();
        repository.GetActiveWorkersAsync(Arg.Any<CancellationToken>())
            .Returns(new[]
            {
                CreateDefinition("kafka-consumer", "KafkaConsumer", "*/1 * * * *"),
                CreateDefinition("database-producer", "DatabaseProducer", "*/1 * * * *")
            });

        var handler = Substitute.For<IWorkerHandler>();
        handler.WorkerType.Returns("DatabaseProducer");
        var registry = new WorkerRegistry();
        registry.Register(handler);

        var engine = new WorkerEngine(
            registry,
            repository,
            Substitute.For<ILogger<WorkerEngine>>(),
            timeProvider: new FixedTimeProvider(new DateTimeOffset(2026, 1, 1, 12, 0, 5, TimeSpan.Zero)));

        await engine.ExecuteDueWorkersAsync(CancellationToken.None);

        await handler.Received(1).ExecuteAsync(
            Arg.Is<WorkerContext>(context => context.WorkerType == "DatabaseProducer"),
            Arg.Any<CancellationToken>());
    }

    private static WorkerDefinition CreateDefinition(string name, string workerType, string cronExpression)
    {
        return new WorkerDefinition
        {
            Name = name,
            WorkerType = workerType,
            CronExpression = cronExpression,
            RetryCount = 1,
            IsEnabled = true
        };
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
