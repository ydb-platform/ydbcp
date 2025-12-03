-- +goose Up
ALTER TABLE Backups
    ADD COLUMN encryption_algorithm String,
    ADD COLUMN kms_key_id String;

ALTER TABLE Operations
    ADD COLUMN encryption_algorithm String,
    ADD COLUMN kms_key_id String;

ALTER TABLE BackupSchedules
    ADD COLUMN encryption_algorithm String,
    ADD COLUMN kms_key_id String;

-- +goose Down
ALTER TABLE BackupSchedules
    DROP COLUMN kms_key_id,
    DROP COLUMN encryption_algorithm;

ALTER TABLE Operations
    DROP COLUMN kms_key_id,
    DROP COLUMN encryption_algorithm;

ALTER TABLE Backups
    DROP COLUMN kms_key_id,
    DROP COLUMN encryption_algorithm;

