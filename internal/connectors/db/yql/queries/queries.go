package queries

import (
	"fmt"
	"ydbcp/internal/types"
)

var (
	ListSchedulesQuery = fmt.Sprintf(
		`$last_successful_backup_id = SELECT schedule_id, MAX_BY(b.created_at, b.completed_at) AS recovery_point, 
MAX_BY(b.id, 
b.completed_at) AS last_successful_backup_id FROM Backups AS b WHERE b.status = '%s' GROUP BY schedule_id;
$last_backup_id = SELECT schedule_id AS schedule_id_2, MAX_BY(b.id, b.completed_at) AS last_backup_id FROM Backups AS b GROUP BY schedule_id;

SELECT * FROM BackupSchedules AS schedules 
LEFT JOIN $last_successful_backup_id AS b1 ON schedules.id = b1.schedule_id
LEFT JOIN $last_backup_id AS b2 ON schedules.id = b2.schedule_id_2
`, types.BackupStateAvailable,
	)
	GetScheduleQuery = fmt.Sprintf(
		`$rpo_info = SELECT 
    <|
        recovery_point: MAX_BY(b.created_at, b.completed_at),
        last_successful_backup_id: MAX_BY(b.id, b.completed_at)
    |> FROM Backups AS b WHERE b.status = '%s' AND b.schedule_id = $schedule_id;

$last_backup_id = SELECT MAX_BY(b.id, b.completed_at) AS last_backup_id FROM Backups AS b WHERE b.schedule_id = $schedule_id;

SELECT s.*, $last_backup_id AS last_backup_id, $rpo_info.recovery_point AS recovery_point, $rpo_info.last_successful_backup_id AS last_successful_backup_id FROM BackupSchedules AS s WHERE s.id = $schedule_id
`, types.BackupStateAvailable,
	)
	GetBackupsToDeleteQuery = fmt.Sprintf(
		`SELECT * FROM Backups VIEW idx_expire_at WHERE status != '%s' and status != '%s' AND expire_at < CurrentUtcTimestamp() LIMIT 100`,
		types.BackupStateDeleted,
		types.BackupStateDeleting,
	)
)
