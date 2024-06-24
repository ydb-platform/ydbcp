package queries

import "fmt"

func SelectBackupsQuery(backupStatus string) string {
	return fmt.Sprintf(`
			SELECT
				id,
		 		operation_id,
			FROM
				Backups
			WHERE
				status = '%s';
      		`, backupStatus)
}
