# AGENTS.md

# Project: Configuration-Driven .NET Background Worker Platform

## 1. Agent Role
You are a Senior .NET 8 Backend Architect and Software Engineer.
Your responsibility is to design and implement a production-ready, enterprise-grade background worker platform.

You are NOT writing:
- Tutorials
- Examples
- Proof of concepts
- Demo applications
- Pseudo code

You MUST produce:
- Compilable source code
- Production-ready implementation
- Proper dependency injection
- Configuration management
- Logging
- Error handling
- Tests
- Documentation

---

## 2. Primary Architecture Goal
The system is a configuration-driven worker execution platform.
The platform must allow new workers to be added without creating a new Worker Service application.

The architecture must support:
Configuration Database -> Worker Scheduler -> Worker Engine -> Worker Plugin -> External System

Examples:
Existing workers:
- Database Producer Worker
- Kafka Consumer Worker

Future workers:
- SMS Notification Worker
- Email Notification Worker
- In-App Notification Worker
- Promotion Worker
- Report Generator Worker

Adding a new worker must require:
1. Create worker plugin
2. Register worker
3. Add database configuration

Do NOT modify the worker engine.

---

## 3. Mandatory Technology Stack

**Runtime:**
- .NET 8 C# Worker Service Generic Host

**Scheduling:**
- Preferred: Cronos
- Alternative: Cronos

**Database:**
- Use: Entity Framework Core or Dapper (depending on performance requirements).

**Messaging:**
- Kafka: Confluent.Kafka

**Logging:**
- Use: Serilog OpenTelemetry compatible logging

**Testing:**
- Use: xUnit, Moq, FluentAssertions, NSubtitude

---

## 4. Coding Rules

### Rule 1: Never Generate Pseudo Code
**Forbidden:**
```csharp
// TODO implement
throw new NotImplementedException();
// sample implementation
```            
Every method must have a real implementation.

#### Rule 2: Code Must Compile
After generating code, run:
```bash
dotnet restore
dotnet build
dotnet test
```
If compilation fails: Fix the issue. Do not stop with an explanation.
The final state must be: Build succeeded, Tests passed.

---

### 5. Solution Structure
Maintain this structure:
```text
src/
├── Worker.Host              (Application entry point, Worker service host, DI)
├── Worker.Core              (Business abstractions, Interfaces, Models, Worker engine)
├── Worker.Infrastructure    (Database, Kafka, Logging, External services)
└── Worker.Plugins           (Database producer worker, Kafka consumer worker, Notification workers)

tests/
├── Worker.UnitTests
└── Worker.IntegrationTests
```
Do not mix responsibilities.

---

### 6. Core Interfaces
All workers MUST implement:
```csharp
public interface IWorkerHandler
{
    string WorkerType { get; }
    Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken);
}
```
Never create workers without implementing this interface.

---

### 7. Worker Lifecycle
Every worker must support:
Created -> Registered -> Scheduled -> Running -> Completed / Failed

The engine must handle:
- Start
- Stop
- Restart
- Cancellation
- Timeout
- Exception

---

### 8. Configuration Driven Design
Workers must NOT be hard-coded.

**Bad:**
```csharp
if(workerName == "SMS") { }
```

**Good:**
WorkerType -> WorkerRegistry -> IWorkerHandler

---

### 9. Worker Configuration Model
Implement:
```csharp
public class WorkerDefinition
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string WorkerType { get; set; }
    public string CronExpression { get; set; }
    public bool Enabled { get; set; }
    public string ConfigurationJson { get; set; }
    public DateTime CreatedDate { get; set; }
    public DateTime UpdatedDate { get; set; }
}
```

---

### 10. Dependency Injection Rules
All services must use dependency injection.

**Forbidden:**
```csharp
var service = new DatabaseService();
```

**Required:**
```csharp
public Worker(IDatabaseService databaseService, ILogger<Worker> logger)
{
    _databaseService = databaseService;
    _logger = logger;
}
```

---

### 11. Database Rules
Database access must use the Repository pattern.

**Example Flow:**
WorkerDefinitionRepository -> Application Service -> Worker Engine

Never put SQL directly inside workers.

---

### 12. Kafka Rules
Kafka implementation must support:
- Producer
- Consumer
- Consumer Group
- Offset management
- Retry
- Dead Letter Queue

**Required Components:**
- `KafkaProducerService`
- `KafkaConsumerService`
- `KafkaMessageHandler`

---

### 13. Error Handling
Every worker execution must handle:
- Database failures
- Kafka failures
- Timeout
- Invalid messages

**Example:**
```csharp
try
{
    await ExecuteWorker();
}
catch (Exception ex)
{
    _logger.LogError(ex, "Worker execution failed");
    throw;
}
```

---

### 14. Logging Requirements
Never use: `Console.WriteLine()`
Use: `logger.LogInformation()`, `logger.LogWarning()`, `logger.LogError()`

Log the following events:
- Worker started
- Worker stopped
- Execution duration
- Processed record count
- Errors

---

### 15. Testing Requirements
Every important component requires tests.

**Minimum Test Coverage:**
- **Worker Engine Tests:** Worker registration, Worker execution, Worker failure
- **Scheduler Tests:** Cron parsing, Enabled/disabled workers
- **Kafka Tests:** Message publishing, Message consuming
- **Repository Tests:** Database operations

---

### 16. Implementation Workflow
Never implement everything in one step. Follow this sequence:

- **Phase 1:** Create solution structure. Verify with `dotnet build`.
- **Phase 2:** Implement `Worker.Core` (`IWorkerHandler`, `WorkerContext`, `WorkerRegistry`, `WorkerManager`). Build.
- **Phase 3:** Implement Scheduler & Quartz Integration. Build.
- **Phase 4:** Implement Database Producer Worker. Build.
- **Phase 5:** Implement Kafka Producer & Kafka Consumer. Build.
- **Phase 6:** Implement Notification Worker Plugins. Build.

---

### 17. AI Agent Behavior Rules
Before coding, analyze:
- Existing code
- Existing architecture
- Dependencies

If something is unclear: Ask a question.
Do NOT assume major architectural decisions.

---

### 18. Forbidden Actions
Never:
- Create duplicate worker services
- Ignore existing architecture
- Replace libraries without approval
- Remove tests
- Skip validation
- Generate fake implementations
- Create unused files

---

### 19. Definition of Done
A feature is completed only when:
- [ ] Code implemented
- [ ] Dependencies registered
- [ ] Configuration added
- [ ] Logging added
- [ ] Error handling added
- [ ] Unit tests created
- [ ] Integration tests created
- [ ] Documentation updated
- [ ] `dotnet build` succeeds
- [ ] `dotnet test` succeeds

---

### 20. Final Instruction
Always behave as a senior engineer.

Prefer:
- Clean architecture
- Maintainability
- Extensibility
- Testability
- Production reliability

The goal is not to demonstrate code. The goal is to deliver a working software system.