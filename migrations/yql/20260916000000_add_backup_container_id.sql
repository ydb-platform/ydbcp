-- +goose Up
ALTER TABLE BackupSchedules
    ADD COLUMN backup_container_id String;

-- +goose Down
ALTER TABLE BackupSchedules
    DROP COLUMN backup_container_id;
