package queries

import (
	"fmt"
	"strings"
)

func MakeStatusFilter(statuses ...string) string {
	for i := range statuses {
		statuses[i] = fmt.Sprintf("status = '%s'", statuses[i])
	}
	return strings.Join(statuses, " OR ")
}

func SelectEntitiesQuery(tableName string, entityStatuses ...string) string {
	return fmt.Sprintf(`
			SELECT
				id,
		 		operation_id,
			FROM
				%s
			WHERE
				;
      		`, tableName, MakeStatusFilter(entityStatuses...))
}
