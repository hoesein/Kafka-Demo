INSERT INTO worker_definitions (
    name,
    worker_type,
    cron_expression,
    enabled,
    retry_count,
    timeout_seconds,
    batch_size,
    configuration_json,
    status
)
VALUES
    (
        'database-producer',
        'DatabaseProducer',
        '*/5 * * * *',
        TRUE,
        3,
        120,
        250,
        '{"Query":"SELECT * FROM notification_queue WHERE status = ''NEW''","KafkaTopic":"notification-topic","SourceConnectionString":"Host=db-one;Port=5432;Database=db_one;Username=worker;Password=workerpass;"}'::jsonb,
        'active'
    ),
    (
        'kafka-consumer',
        'KafkaConsumer',
        '*/2 * * * *',
        TRUE,
        2,
        90,
        100,
        '{"Topic":"sms-topic","ConsumerGroup":"sms-service","TargetConnectionString":"Host=db-two;Port=5432;Database=db_two;Username=worker;Password=workerpass;"}'::jsonb,
        'active'
    ),
    (
        'notification',
        'Notification',
        '*/10 * * * *',
        TRUE,
        3,
        60,
        50,
        '{"Channel":"email","Recipient":"ops@example.com","Message":"Scheduled worker notification"}'::jsonb,
        'active'
    )
ON CONFLICT (name) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'Query', 'SELECT * FROM notification_queue WHERE status = ''NEW'''
FROM worker_definitions
WHERE name = 'database-producer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'KafkaTopic', 'notification-topic'
FROM worker_definitions
WHERE name = 'database-producer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'SourceConnectionString', 'Host=db-one;Port=5432;Database=db_one;Username=worker;Password=workerpass;'
FROM worker_definitions
WHERE name = 'database-producer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'Topic', 'sms-topic'
FROM worker_definitions
WHERE name = 'kafka-consumer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'ConsumerGroup', 'sms-service'
FROM worker_definitions
WHERE name = 'kafka-consumer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'TargetConnectionString', 'Host=db-two;Port=5432;Database=db_two;Username=worker;Password=workerpass;'
FROM worker_definitions
WHERE name = 'kafka-consumer'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'Channel', 'email'
FROM worker_definitions
WHERE name = 'notification'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO worker_definition_settings (worker_definition_id, setting_key, setting_value)
SELECT id, 'Recipient', 'ops@example.com'
FROM worker_definitions
WHERE name = 'notification'
ON CONFLICT (worker_definition_id, setting_key) DO NOTHING;

INSERT INTO notification_queue (message_key, payload, status)
VALUES
    ('welcome-1', '{"type":"email","to":"ops@example.com","subject":"Worker startup"}'::jsonb, 'NEW'),
    ('welcome-2', '{"type":"sms","to":"+15551234567","body":"Provider started"}'::jsonb, 'NEW');