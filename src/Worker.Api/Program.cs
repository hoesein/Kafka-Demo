using Worker.Api;
using Worker.Contracts;
using Worker.Infrastructure.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();
builder.Services.AddWorkerPlatform(builder.Configuration);
builder.Services.AddSingleton<IWorkerExecutionHistoryStore, InMemoryWorkerExecutionHistoryStore>();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
}

app.UseSwagger();
app.UseSwaggerUI();

app.MapHealthChecks("/health");

app.MapGet("/workers/status", async (IWorkerConfigurationRepository repository, IWorkerScheduler scheduler) =>
{
    var workers = await repository.GetActiveWorkersAsync();
    var summary = workers.Select(w => new
    {
        w.Name,
        w.WorkerType,
        w.CronExpression,
        w.Enabled,
        w.IsEnabled,
        w.RetryCount,
        w.TimeoutSeconds,
        w.BatchSize
    }).ToArray();

    return Results.Ok(new
    {
        Status = "Healthy",
        RegisteredCount = scheduler.GetDefinitions().Count,
        ActiveCount = summary.Length,
        Workers = summary
    });
});

app.MapGet("/workers/history", (IWorkerExecutionHistoryStore historyStore) =>
{
    var history = historyStore.GetHistory()
        .Select(x => new
        {
            x.WorkerName,
            x.WorkerType,
            x.CorrelationId,
            x.StartedAt,
            x.FinishedAt,
            x.Succeeded,
            x.ProcessedRecordCount,
            x.ErrorMessage
        })
        .ToArray();

    return Results.Ok(new
    {
        Total = history.Length,
        History = history
    });
});

app.Run();
