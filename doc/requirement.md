# Requirement: Configuration-Driven .NET Background Worker Platform

## 1. Project Overview

Build a reusable, extensible .NET Core Background Worker Platform that supports multiple background processing workers without requiring developers to create a new worker service implementation from scratch for every new business requirement.

The platform should support:

- Scheduled data fetching workers
- Kafka producer workers
- Kafka consumer workers
- Notification workers
- Future business-specific workers

New workers should be created mainly through configuration and plugin registration rather than rebuilding worker infrastructure.

---

# 2. Business Goal

Current problem:

Every new background process requires:

- Creating a new Worker Service project
- Implementing scheduling logic
- Implementing retry handling
- Implementing Kafka communication
- Implementing logging
- Implementing database access patterns
- Deploying another service

This creates duplicated code and increases maintenance cost.

---

## Target Solution

Create a generic Worker Framework where:

1. Core worker engine is developed once.
2. Worker behavior is defined by configuration.
3. New workers can be added by:
   - Adding database configuration
   - Registering worker type
   - Providing execution handler/plugin
   - Defining schedule using cron expression

Example:

Adding a new:

- SMS notification worker
- In-app notification worker
- Promotion notification worker

should not require creating a new worker service from zero.

---

# 3. High-Level Architecture

```
+-----------------------------+
|     Worker Configuration    |
|          Database           |
|                             |
| - Worker Definition         |
| - Cron Expression           |
| - Worker Type               |
| - Worker Settings           |
+-------------+---------------+
              |
              |
              v
+--------------------------------+
|        Worker Scheduler        |
|                                |
| - Cron Scheduler               |
| - Worker Trigger Management    |
| - Dynamic Worker Reload        |
+----------------+---------------+
                 |
                 |
                 v
+------------------------------------------------+
|              .NET Worker Runtime               |
|                                                |
| +--------------------------------------------+ |
| |             Worker Engine                  | |
| |                                            | |
| | - Worker Lifecycle Management              | |
| | - Retry Handling                           | |
| | - Error Handling                           | |
| | - Logging                                  | |
| +--------------------------------------------+ |
|                                                |
| +--------------------------------------------+ |
| |          Worker Plugin Registry            | |
| |                                            | |
| | IWorkerHandler Mapping                     | |
| | WorkerType -> Worker Implementation        | |
| +--------------------------------------------+ |
|                                                |
+----------------------+-------------------------+
                       |
                       |
        +--------------+--------------+
        |                             |
        v                             v
+---------------------+       +----------------------+
| Database Fetch      |       | Notification Worker  |
| Worker              |       | Worker               |
|                     |       |                      |
| WorkerType:         |       | WorkerType:          |
| DatabaseProducer    |       | Notification         |
+----------+----------+       +----------+-----------+
           |                             |
           |                             |
           v                             |
+---------------------+                  |
|        DB-1         |                  |
| Source Database     |                  |
|                     |                  |
| Pending Data        |                  |
| Notification Queue  |                  |
+----------+----------+                  |
           |                             |
           |
           v
+-----------------------------+
|      Kafka Producer         |
|                             |
| - Message Transformation    |
| - Batch Publish             |
| - Retry                     |
+-------------+---------------+
              |
              |
              v
+--------------------------------+
|          Kafka Cluster         |
|                                |
|  Topic: notification-topic     |
|                                |
|  Topic: sms-topic              |
|                                |
|  Topic: promotion-topic        |
+---------------+----------------+
                |
                |
                v
+--------------------------------+
|       Kafka Consumer Workers   |
|                                |
| +----------------------------+ |
| | SMS Consumer Worker        | |
| |                            | |
| | Consume SMS Event          | |
| +----------------------------+ |
|                                |
| +----------------------------+ |
| | In-App Notification Worker | |
| |                            | |
| | Consume App Event          | |
| +----------------------------+ |
|                                |
| +----------------------------+ |
| | Promotion Worker           | |
| |                            | |
| | Consume Promotion Event    | |
| +----------------------------+ |
+---------------+----------------+
                |
                |
                v
+-----------------------------+
|          DB-2               |
|   Destination Database      |
|                             |
| - SMS History               |
| - Notification Status       |
| - Processing Result         |
+-----------------------------+



+-------------------------------------------------+
|              Operations & Monitoring            |
|                                                 |
|  +----------------+   +---------------------+   |
|  | Health Check   |   | Monitoring Dashboard |  |
|  +----------------+   +---------------------+   |
|                                                 |
|  +--------------------------------------------+ |
|  | Structured Logging (Serilog/OpenTelemetry) | |
|  +--------------------------------------------+ |
+-------------------------------------------------+
```

---

# 4. Core Components


## 4.1 Worker Engine

A generic .NET BackgroundService implementation responsible for:

- Worker lifecycle management
- Scheduling
- Execution control
- Logging
- Retry handling
- Exception management
- Health monitoring


Example:

```csharp
public class WorkerEngine : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Load worker configurations
        // Start scheduled workers
        // Monitor worker execution
    }
}
```

---

# 5. Worker Definition Model

Worker should be defined by database configuration.
Example table:

| Column | Description |
| ------ |:-----------:|
| Id | Worker identifier |
| Name | Worker Name |
| WorkerType | Handler Type |
| Enabled | Enable/disable worker |
| CronExpression | Execution schedule |
| ConfigurationJson | Worker specific settings |
| Retry Count | Retry Policy |
| TimeoutSeconds | Execution timeout |
| CreatedDate | Creation Date |
| UpdatedDate | Last Update |

Example Data:

```json
{
    "Name": "SMS Consumer Worker",
    "WorkerType": "KafkaConsumer",
    "CronExpression": "*/10 * * * *",
    "Enabled": true,
    "ConfigurationJson":
    {
        "Topic":"sms-topic",
        "Database":"db2" 
    }
}
```
---

# 6. Worker Execution Model

Every worker must implement a common interface.
Example: 
``` C#
public interface IWorkerHandler
{
    string WorkerType { get; }
    Task ExecuteAsync(WorkerContext context, CancellationToken ct);
}
```
---

# 7. Worker Plugin System
New Worker types should be implemented as plugins.
Example:

Existing:
```
Workers
 |
 +-- DbProducerWorker
 |
 +-- KafkaConsumerWorker
 ```

Future
```
Workers
 |
 +-- DbProducerWorker
 |
 +-- KafkaConsumerWorker
 |
 +-- SmsNotificationWorker
 |
 +-- InAppNotificationWorker
 |
 +-- PromotionNotificationWorker
 ```

 Adding a worker requires:
 1. Create handler class
 Example:
 ``` C#
 public class PromotionNotifactionWorker: IWorkerHandler
 {
    public string WorkerType => "PromotionNotification";
    public async Task ExecuteAsync(WorkerContext context, CancellationToken ct)
    {
        //  
    }
 }
 ```

2. Register worker:
``` C#
services.AddWorker<PromotionNotificationWorker>();
```

3. Add database configuration.
No Shceuler changes.
No infrastructure changes.

---

# 8. Database Producer Worker requirement


## Responsibility
Fetch data from DB-1 periodically.

Process:
```
DB-1
 |
 |
Query Pending Data
 |
 |
Transform Message
 |
 |
Publish Kafka Message
```

Configuration example:
``` json
{
 "WorkerType":"DatabaseProducer",
 "Database":"db1",
 "Query":
 "select * from notification_queue where status='NEW'",
 "KafkaTopic":"notification-topic"
}
```

Requirements:
- Support configurable SQL/query source
- Support batch processing
- Support pagination
- Support retry
- Support duplicate prevention

---

# 9. Kafka Consumer Worker Requirement


## Responsibility
Consume Kafka messages.

Process:
```
Kafka Topic
 |
 |
Consume Message
 |
 |
Validate
 |
 |
Store DB-2
```

Configuration example:
``` json
{
 "WorkerType":"KafkaConsumer",
 "Topic":"sms-topic",
 "ConsumerGroup":"sms-service",
 "TargetDatabase":"db2"
}
```

Requirements:
- Consumer group support
- Offset management
- Retry mechanism
- Dead Letter Queue support
- Idempotent processing

---

# 10. Scheduling Requirement

The system must support dynamic scheduling.

Example:

Database:
```
WorkerName              Cron

SMS Worker              */5 * * * *

Promotion Worker        0 */1 * * *

Cleanup Worker          0 0 * * *
```

Scheduler requirements:
- Cron expression support
- Enable/disable worker dynamically
- Reload configuration without redeploy
- Prevent duplicated execution
- Support mulitple workers simultaneously

Recommended library:
- Cronos

---

# 11. Configuration Management

Worker configuration should support:

## Global Configuration

Example:
``` json
{
 "Kafka":{
   "BootstrapServers":"localhost:9092"
 },

 "Database":{
   "ConnectionPoolSize":100
 }
}
```

## Worker Specific Configuration

Stored in database:
``` json
{
 "BatchSize":500,
 "Retry":3,
 "Topic":"sms-topic"
}
```

---

# 12. Reliability Requirements

The platform must support:

## Retry

Example:
```
Worker failed
 |
Retry 1
 |
Retry 2
 |
Retry 3
 |
Dead Letter Queue
```

## Logging

Use structed logging:

Recommended:
- Serilog
- OpenTelemetry

Log:
- Worker started
- Worker stopped
- Execution duration
- Message count
- Error details

## Monitoring

Expose:
```
/health

/workers/status

/workers/history
```

Information:
- Running Workers
- Last execution time
- Last success
- Last failure
- Processing count

---

# 13. Security Requirement

Must support:
- Secure database connection
- Kafka authentication
- Secret management
- Environment base configuration

Recommended:
- Azure Key Vault
- ??
- ??

---

# 14. Deployment Requirement

The platform should support:
```
Docker Container

        |
        |
Kubernetes Deployment

        |
        |
Multiple Worker Instances
```

Must support:
- Horizontal scaling
- Graceful shutdown
- Health check
- Configuration reload
- Seperate Docker service for each Worker

---

# 15. Agentic AI Development Instructions

When implementing this requirement:

## AI Agent Responsibilities
The AI Agent should:
- Analyze existing project structure
- Create reusable worker framework
- Avoid creating duplicated worker services.
- Prefer configuration-driven design.
- Follow SOLID principles.
- Create extensible interfaces.
- Provie unit tests.
- Provide integration tests.

---

# 16. Expected Project Structure

```
doc
│
├── requirement.md
src
│
├── Worker.Host
│
├── Worker.Core
│
├── Worker.Infrastructure
│
├── Worker.Plugins
│
├── Worker.Contracts
│
└── Worker.Api

```

---

## Worker.Host

Responsible for running the application
Example:
```
Worker.Host
 |
 +-- Program.cs
 +-- WorkerEngine.cs
 +-- SchedulerService.cs
```

---

## Worker.Core

Contains the framework.
Example:

```
Worker.Core

Interfaces
 |
 +-- IWorkerHandler.cs

Models
 |
 +-- WorkerDefinition.cs

Services
 |
 +-- WorkerManager.cs
```

---

## Worker.Contracts

Shared contracts.
Example:

``` C#
public interface IWorkerHandler
{
    string WorkerType { get; }

    Task ExecuteAsync(WorkerContext context, CancellationToken cancellationToken);
}
```

---

## Worker.Plugins

Business workers.
Example:

```
Worker.Plugins

DatabaseProducer
 |
 +-- DatabaseProducerWorker.cs


KafkaConsumer
 |
 +-- KafkaConsumerWorker.cs


Notification
 |
 +-- SmsNotificationWorker.cs
```

## Worker.Api

Responsible for Monitoring through APIs.

---

# 17. Future Extension Examples

The system should allow adding:

## Email Notification Worker
Configuration:
``` json
{
 "WorkerType":"EmailNotification",
 "Cron":"*/1 * * * *"
}
```

## Mobile Push Notification Worker
``` json
{
 "WorkerType":"PushNotification",
 "Cron":"*/10 * * * *"
}
```

## Report Generation Worker
``` json 
{
 "WorkerType":"ReportGenerator",
 "Cron":"0 2 * * * *"
}
```

without changing the worker engine.

---

# Final Acceptance Criteria

The solution is accepted when:
[ ] A new worker can be added without creating a new Worker Service project.
[ ] Worker scheduling is controlled by database configuration.
[ ] Cron expressions control execution timing.
[ ] Kafka producer/consumer are reusable components.
[ ] Worker behavior is implemented through plugins/handlers.
[ ] Configuration changes do not require application redeployment.
[ ] System supports future notification workers.
[ ] Logging, retry, monitoring, and health checks are included.

