using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Worker.Contracts;

namespace Worker.Infrastructure;

public sealed class DatabaseWorkerConfigurationRepository : IWorkerConfigurationRepository
{
    private readonly WorkerPlatformOptions _options;
    private readonly ILogger<DatabaseWorkerConfigurationRepository> _logger;

    public DatabaseWorkerConfigurationRepository(
        IOptions<WorkerPlatformOptions> options,
        ILogger<DatabaseWorkerConfigurationRepository> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyCollection<WorkerDefinition>> GetActiveWorkersAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(_options.Database.DefaultConnectionString))
        {
            _logger.LogWarning("No default connection string configured for worker definitions.");
            return Array.Empty<WorkerDefinition>();
        }

        try
        {
            await using var connection = new NpgsqlConnection(_options.Database.DefaultConnectionString);
            await connection.OpenAsync(cancellationToken);

            return await LoadDefinitionsAsync(
                connection,
                "WHERE wd.enabled = true AND wd.status = 'active'",
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load worker definitions from PostgreSQL.");
            return Array.Empty<WorkerDefinition>();
        }
    }

    public async Task<WorkerDefinition?> GetByNameAsync(string workerName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(workerName))
        {
            throw new ArgumentException("Worker name is required.", nameof(workerName));
        }

        if (string.IsNullOrWhiteSpace(_options.Database.DefaultConnectionString))
        {
            return null;
        }

        try
        {
            await using var connection = new NpgsqlConnection(_options.Database.DefaultConnectionString);
            await connection.OpenAsync(cancellationToken);

            var workers = await LoadDefinitionsAsync(
                connection,
                "WHERE wd.name = @name",
                cancellationToken,
                new NpgsqlParameter("name", workerName));

            return workers.FirstOrDefault();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load worker definition {WorkerName} from PostgreSQL.", workerName);
            return null;
        }
    }

    public async Task SaveAsync(WorkerDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        cancellationToken.ThrowIfCancellationRequested();

        definition.ApplyConfigurationJson();

        if (string.IsNullOrWhiteSpace(_options.Database.DefaultConnectionString))
        {
            throw new InvalidOperationException("A default PostgreSQL connection string is required to save worker definitions.");
        }

        await using var connection = new NpgsqlConnection(_options.Database.DefaultConnectionString);
        await connection.OpenAsync(cancellationToken);

        var isUpdate = definition.Id != Guid.Empty;

        if (!isUpdate)
        {
            await using (var identityCommand = new NpgsqlCommand(
                @"SELECT id FROM worker_definitions WHERE name = @name LIMIT 1;",
                connection))
            {
                identityCommand.Parameters.AddWithValue("name", definition.Name);
                var existingId = await identityCommand.ExecuteScalarAsync(cancellationToken);
                if (existingId is Guid guid)
                {
                    definition.Id = guid;
                    isUpdate = true;
                }
            }
        }

        if (isUpdate)
        {
            await using var updateCommand = new NpgsqlCommand(
                @"
                UPDATE worker_definitions
                SET name = @name,
                    worker_type = @workerType,
                    cron_expression = @cronExpression,
                    enabled = @enabled,
                    retry_count = @retryCount,
                    timeout_seconds = @timeoutSeconds,
                    batch_size = @batchSize,
                    configuration_json = @configurationJson::jsonb,
                    updated_at = now()
                WHERE id = @id;",
                connection);

            updateCommand.Parameters.AddWithValue("id", definition.Id);
            updateCommand.Parameters.AddWithValue("name", definition.Name);
            updateCommand.Parameters.AddWithValue("workerType", definition.WorkerType);
            updateCommand.Parameters.AddWithValue("cronExpression", definition.CronExpression);
            updateCommand.Parameters.AddWithValue("enabled", definition.IsEnabled);
            updateCommand.Parameters.AddWithValue("retryCount", definition.RetryCount);
            updateCommand.Parameters.AddWithValue("timeoutSeconds", definition.TimeoutSeconds);
            updateCommand.Parameters.AddWithValue("batchSize", definition.BatchSize);
            updateCommand.Parameters.AddWithValue("configurationJson", definition.ConfigurationJson ?? "{}");
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            await using var insertCommand = new NpgsqlCommand(
                @"
                INSERT INTO worker_definitions (name, worker_type, cron_expression, enabled, retry_count, timeout_seconds, batch_size, configuration_json)
                VALUES (@name, @workerType, @cronExpression, @enabled, @retryCount, @timeoutSeconds, @batchSize, @configurationJson::jsonb)
                RETURNING id;",
                connection);

            insertCommand.Parameters.AddWithValue("name", definition.Name);
            insertCommand.Parameters.AddWithValue("workerType", definition.WorkerType);
            insertCommand.Parameters.AddWithValue("cronExpression", definition.CronExpression);
            insertCommand.Parameters.AddWithValue("enabled", definition.IsEnabled);
            insertCommand.Parameters.AddWithValue("retryCount", definition.RetryCount);
            insertCommand.Parameters.AddWithValue("timeoutSeconds", definition.TimeoutSeconds);
            insertCommand.Parameters.AddWithValue("batchSize", definition.BatchSize);
            insertCommand.Parameters.AddWithValue("configurationJson", definition.ConfigurationJson ?? "{}");
            var insertedId = await insertCommand.ExecuteScalarAsync(cancellationToken);
            if (insertedId is Guid guid)
            {
                definition.Id = guid;
            }
        }

        await SaveSettingsAsync(connection, definition, cancellationToken);
    }

    private async Task<IReadOnlyCollection<WorkerDefinition>> LoadDefinitionsAsync(
        NpgsqlConnection connection,
        string whereClause,
        CancellationToken cancellationToken,
        params NpgsqlParameter[] parameters)
    {
        var sql = @"
            SELECT
                wd.id,
                wd.name,
                wd.worker_type,
                wd.cron_expression,
                wd.enabled,
                wd.retry_count,
                wd.timeout_seconds,
                wd.batch_size,
                wd.configuration_json,
                wd.created_at,
                wd.updated_at,
                s.setting_key,
                s.setting_value
            FROM worker_definitions wd
            LEFT JOIN worker_definition_settings s ON s.worker_definition_id = wd.id
            " + whereClause + @"
            ORDER BY wd.name, s.setting_key;";

        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var workers = new Dictionary<Guid, WorkerDefinition>(GuidComparer.Instance);

        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.GetGuid(reader.GetOrdinal("id"));

            if (!workers.TryGetValue(id, out var definition))
            {
                definition = new WorkerDefinition
                {
                    Id = id,
                    Name = reader.GetString(reader.GetOrdinal("name")),
                    WorkerType = reader.GetString(reader.GetOrdinal("worker_type")),
                    CronExpression = reader.GetString(reader.GetOrdinal("cron_expression")),
                    IsEnabled = reader.GetBoolean(reader.GetOrdinal("enabled")),
                    RetryCount = reader.GetInt32(reader.GetOrdinal("retry_count")),
                    TimeoutSeconds = reader.GetInt32(reader.GetOrdinal("timeout_seconds")),
                    BatchSize = reader.GetInt32(reader.GetOrdinal("batch_size")),
                    ConfigurationJson = reader.IsDBNull(reader.GetOrdinal("configuration_json")) ? "{}" : reader.GetString(reader.GetOrdinal("configuration_json")),
                    CreatedDate = reader.GetDateTime(reader.GetOrdinal("created_at")),
                    UpdatedDate = reader.IsDBNull(reader.GetOrdinal("updated_at")) ? null : reader.GetDateTime(reader.GetOrdinal("updated_at"))
                };

                definition.ApplyConfigurationJson();
                workers[id] = definition;
            }

            if (!reader.IsDBNull(reader.GetOrdinal("setting_key")) && !reader.IsDBNull(reader.GetOrdinal("setting_value")))
            {
                var settingKey = reader.GetString(reader.GetOrdinal("setting_key"));
                var settingValue = reader.GetString(reader.GetOrdinal("setting_value"));
                definition.Settings[settingKey] = settingValue;
            }
        }

        return workers.Values.ToArray();
    }

    private static async Task SaveSettingsAsync(NpgsqlConnection connection, WorkerDefinition definition, CancellationToken cancellationToken)
    {
        await using var deleteCommand = new NpgsqlCommand(
            "DELETE FROM worker_definition_settings WHERE worker_definition_id = @definitionId;",
            connection);
        deleteCommand.Parameters.AddWithValue("definitionId", definition.Id);
        await deleteCommand.ExecuteNonQueryAsync(cancellationToken);

        if (definition.Settings.Count == 0)
        {
            return;
        }

        foreach (var setting in definition.Settings)
        {
            await using var insertCommand = new NpgsqlCommand(
                @"INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value) VALUES (@definitionId, @key, @value);",
                connection);
            insertCommand.Parameters.AddWithValue("definitionId", definition.Id);
            insertCommand.Parameters.AddWithValue("key", setting.Key);
            insertCommand.Parameters.AddWithValue("value", setting.Value);
            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private sealed class GuidComparer : IEqualityComparer<Guid>
    {
        public static readonly GuidComparer Instance = new();

        public bool Equals(Guid x, Guid y) => x == y;

        public int GetHashCode(Guid obj) => obj.GetHashCode();
    }
}
