using System.Text.Json;

namespace Worker.Contracts;

public sealed class WorkerDefinition
{
    public Guid Id { get; set; } = Guid.Empty;

    public string Name { get; set; } = string.Empty;

    public string WorkerName
    {
        get => Name;
        set => Name = value;
    }

    public string WorkerType { get; set; } = string.Empty;

    public string CronExpression { get; set; } = "* * * * *";

    public bool Enabled
    {
        get => IsEnabled;
        set => IsEnabled = value;
    }

    public bool IsEnabled { get; set; } = true;

    public int RetryCount { get; set; } = 3;

    public int TimeoutSeconds { get; set; } = 60;

    public int BatchSize { get; set; } = 100;

    public string ConfigurationJson { get; set; } = "{}";

    public Dictionary<string, string> Settings { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public DateTimeOffset CreatedDate { get; set; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? UpdatedDate { get; set; }

    public string? SourceConnectionString
    {
        get => GetSettingValue("SourceConnectionString", "SourceDatabaseConnectionString", "SourceDbConnectionString", "ConnectionString", "DatabaseConnectionString");
        set => SetSettingValue("SourceConnectionString", value);
    }

    public string? TargetConnectionString
    {
        get => GetSettingValue("TargetConnectionString", "TargetDatabaseConnectionString", "TargetDbConnectionString", "ConnectionString", "DatabaseConnectionString");
        set => SetSettingValue("TargetConnectionString", value);
    }

    public string? DefaultConnectionString
    {
        get => GetSettingValue("DefaultConnectionString", "ConnectionString", "DatabaseConnectionString");
        set => SetSettingValue("DefaultConnectionString", value);
    }

    public void ApplyConfigurationJson()
    {
        if (string.IsNullOrWhiteSpace(ConfigurationJson))
        {
            return;
        }

        try
        {
            using var document = JsonDocument.Parse(ConfigurationJson);
            foreach (var property in document.RootElement.EnumerateObject())
            {
                Settings[property.Name] = property.Value.ToString();
            }
        }
        catch (JsonException)
        {
            // Ignore malformed JSON in the database row and keep the existing settings.
        }
    }

    public string? GetConnectionStringForRole(string role)
    {
        if (string.Equals(role, "source", StringComparison.OrdinalIgnoreCase))
        {
            return SourceConnectionString;
        }

        if (string.Equals(role, "target", StringComparison.OrdinalIgnoreCase))
        {
            return TargetConnectionString;
        }

        return DefaultConnectionString;
    }

    private string? GetSettingValue(params string[] keys)
    {
        foreach (var key in keys)
        {
            if (Settings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }

    private void SetSettingValue(string key, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            if (Settings.ContainsKey(key))
            {
                Settings.Remove(key);
            }

            return;
        }

        Settings[key] = value;
    }
}












































































































































































































































































































































































































































































































