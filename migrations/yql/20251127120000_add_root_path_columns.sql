-- +goose Up
ALTER TABLE Operations
    ADD COLUMN root_path String;

ALTER TABLE BackupSchedules
    ADD COLUMN root_path String;

-- +goose Down
ALTER TABLE BackupSchedules
    DROP COLUMN root_path;

ALTER TABLE Operations
    DROP COLUMN root_path;

