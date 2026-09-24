-- +goose Up
ALTER TABLE replicated_backups
    DROP INDEX idx_container_id;

ALTER TABLE replicated_backups
    ALTER COLUMN container_id DROP NOT NULL;

ALTER TABLE replicated_backups
    ALTER COLUMN database DROP NOT NULL;

ALTER TABLE replicated_backups
    ALTER COLUMN endpoint DROP NOT NULL;

ALTER TABLE replicated_backups
    ADD INDEX idx_container_id GLOBAL ON (container_id);
