package queries

import (
	"context"
	"ydbcp/internal/types"
)

type WriteTableQueryMock struct {
	Operation types.Operation
	Backup    types.Backup
}

func NewWriteTableQueryMock() WriteTableQuery {
	return &WriteTableQueryMock{}
}

func (w *WriteTableQueryMock) FormatQuery(_ context.Context) (*FormatQueryResult, error) {
	return &FormatQueryResult{}, nil
}

func (w *WriteTableQueryMock) WithCreateBackup(backup types.Backup) WriteTableQuery {
	w.Backup = backup
	return w
}

func (w *WriteTableQueryMock) WithCreateOperation(operation types.Operation) WriteTableQuery {
	w.Operation = operation
	return w
}

func (w *WriteTableQueryMock) WithUpdateBackup(backup types.Backup) WriteTableQuery {
	w.Backup = backup
	return w
}

func (w *WriteTableQueryMock) WithUpdateOperation(operation types.Operation) WriteTableQuery {
	w.Operation = operation
	return w
}
