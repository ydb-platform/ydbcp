-- +goose Up
CREATE TABLE replicated_backups (
    id String NOT NULL,
    container_id String NOT NULL,
    database String NOT NULL,
    endpoint String NOT NULL,

    initiated String,
    created_at Timestamp,
    completed_at Timestamp,

    s3_endpoint String,
    s3_region String,
    s3_bucket String,
    s3_path_prefix String,

    status String,
    message String,
    size Int64,
    expire_at Timestamp,

    paths String,
    schedule_id String,

    encryption_algorithm String,
    kms_key_id String,

    INDEX idx_container_id GLOBAL ON (container_id),
    INDEX idx_created_at GLOBAL ON (created_at),
    PRIMARY KEY (id)
);

-- +goose Down
DROP TABLE replicated_backups;
