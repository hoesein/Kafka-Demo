using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Worker.Contracts;
using Worker.Plugins;
using Xunit;

namespace Worker.UnitTests;

public class PluginWorkersTests
{
    [Fact]
    public async Task DatabaseProducerWorker_ShouldFetchRows_AndPublishToKafka()
    {
        var dataSource = Substitute.For<IWorkerDataSource>();
        var kafkaProducer = Substitute.For<IKafkaProducerService>();
        var logger = Substitute.For<ILogger<DatabaseProducerWorker>>();

        var worker = new DatabaseProducerWorker(dataSource, kafkaProducer, logger);
        var context = new WorkerContext
        {
            WorkerName = "DatabaseProducer",
            WorkerType = "DatabaseProducer",
            Metadata = new Dictionary<string, string>
            {
                ["Query"] = "SELECT * FROM notification_queue WHERE status = 'NEW'",
                ["KafkaTopic"] = "notification-topic",
                ["BatchSize"] = "2",
                ["SourceConnectionString"] = "Host=db-one;Database=db_one;"
            }
        };

        dataSource.GetPendingMessagesAsync("DatabaseProducer", "SELECT * FROM notification_queue WHERE status = 'NEW'", 2, "Host=db-one;Database=db_one;", Arg.Any<CancellationToken>())
            .Returns(new[] { "m1", "m2" });

        await worker.ExecuteAsync(context, CancellationToken.None);

        await dataSource.Received(1).GetPendingMessagesAsync(
            "DatabaseProducer",
            "SELECT * FROM notification_queue WHERE status = 'NEW'",
            2,
            "Host=db-one;Database=db_one;",
            Arg.Any<CancellationToken>());

        await kafkaProducer.Received(1).PublishAsync(
            "notification-topic",
            Arg.Is<IReadOnlyCollection<string>>(messages => messages.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task KafkaConsumerWorker_ShouldConsume_AndStoreMessages()
    {
        var consumer = Substitute.For<IKafkaConsumerService>();
        var resultStore = Substitute.For<IWorkerResultStore>();
        var logger = Substitute.For<ILogger<KafkaConsumerWorker>>();

        var worker = new KafkaConsumerWorker(consumer, resultStore, logger);
        var context = new WorkerContext
        {
            WorkerName = "KafkaConsumer",
            WorkerType = "KafkaConsumer",
            Metadata = new Dictionary<string, string>
            {
                ["Topic"] = "sms-topic",
                ["ConsumerGroup"] = "sms-service",
                ["BatchSize"] = "3",
                ["TargetConnectionString"] = "Host=db-two;Database=db_two;"
            }
        };

        consumer.ConsumeAsync("sms-topic", "sms-service", 3, Arg.Any<CancellationToken>())
            .Returns(new[] { "a", "b" });

        await worker.ExecuteAsync(context, CancellationToken.None);

        await consumer.Received(1).ConsumeAsync("sms-topic", "sms-service", 3, Arg.Any<CancellationToken>());
        await resultStore.Received(1).StoreAsync(
            "KafkaConsumer",
            "sms-topic",
            Arg.Is<IReadOnlyCollection<string>>(messages => messages.Count == 2),
            "Host=db-two;Database=db_two;",
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NotificationWorker_ShouldSendMessage_WhenMetadataIsValid()
    {
        var notificationService = Substitute.For<INotificationService>();
        var logger = Substitute.For<ILogger<NotificationWorker>>();

        var worker = new NotificationWorker(notificationService, logger);
        var context = new WorkerContext
        {
            WorkerName = "Notification",
            WorkerType = "Notification",
            Metadata = new Dictionary<string, string>
            {
                ["Channel"] = "sms",
                ["Recipient"] = "+123456789",
                ["Message"] = "order ready"
            }
        };

        await worker.ExecuteAsync(context, CancellationToken.None);

        await notificationService.Received(1).SendAsync(
            "sms",
            "+123456789",
            "order ready",
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task NotificationWorker_ShouldThrow_WhenRecipientOrMessageMissing()
    {
        var notificationService = Substitute.For<INotificationService>();
        var logger = Substitute.For<ILogger<NotificationWorker>>();

        var worker = new NotificationWorker(notificationService, logger);
        var context = new WorkerContext
        {
            WorkerName = "Notification",
            WorkerType = "Notification",
            Metadata = new Dictionary<string, string>
            {
                ["Channel"] = "sms"
            }
        };

        var action = async () => await worker.ExecuteAsync(context, CancellationToken.None);

        await action.Should().ThrowAsync<InvalidOperationException>();
    }
}
