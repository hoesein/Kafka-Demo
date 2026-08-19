namespace Worker.Contracts;

public sealed class WorkerPlatformOptions
{
    public string EnvironmentName { get; set; } = "Development";

    public KafkaSettings Kafka { get; set; } = new();

    public DatabaseSettings Database { get; set; } = new();
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";

    public string SecurityProtocol { get; set; } = "PLAINTEXT";
}

public sealed class DatabaseSettings
{
    public int ConnectionPoolSize { get; set; } = 25;

    public string DefaultConnectionString { get; set; } = "Server=localhost;Database=WorkerDb;Trusted_Connection=True;TrustServerCertificate=True;";

    public bool UseIntegratedSecurity { get; set; } = true;
}