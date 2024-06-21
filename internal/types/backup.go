package types

import "github.com/google/uuid"

type Backup struct {
	Backup_id    uuid.UUID
	Operation_id uint64
}
