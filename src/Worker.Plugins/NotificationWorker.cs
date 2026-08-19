using Microsoft.Extensions.Logging;
using Worker.Contracts;

namespace Worker.Plugins;

public sealed class NotificationWorker : IWorkerHandler
{
    private readonly INotificationService _notificationService;
    private readonly ILogger<NotificationWorker> _logger;

    public NotificationWorker(INotificationService notificationService, ILogger<NotificationWorker> logger)
    {
        _notificationService = notificationService;
        _logger = logger;
    }

    public string WorkerType => "Notification";

    public async Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var channel = context.Metadata.TryGetValue("Channel", out var value) ? value : "email";
        var recipient = context.Metadata.TryGetValue("Recipient", out var recipientValue) ? recipientValue : string.Empty;
        var message = context.Metadata.TryGetValue("Message", out var messageValue) ? messageValue : string.Empty;

        if (string.IsNullOrWhiteSpace(recipient) || string.IsNullOrWhiteSpace(message))
        {
            throw new InvalidOperationException("Notification worker requires a recipient and message in metadata.");
        }

        _logger.LogInformation("Sending notification via {Channel} to {Recipient}.", channel, recipient);
        await _notificationService.SendAsync(channel, recipient, message, cancellationToken);
    }
}
