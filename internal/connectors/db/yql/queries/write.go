package queries

func UpdateBackupQuery() string {
	return `
		UPSERT INTO Backups (id, status) VALUES ($id, $status);
	`
}

func UpdateOperationQuery() string {
	return `
		UPSERT INTO Operations (id, status, message) VALUES ($id, $status, $message);
	`
}

func CreateOperationQuery() string {
	//skipped completed_at and paths
	return `
		UPSERT INTO Operations (
			id,
			type,
			container_id,
			database,
			backup_id,
			initiated,
			created_at,
			status,
			operation_id
		) VALUES (
			$id,
			$type,
			$container_id,
			$database,
			$backup_id,
			$initiated,
			$created_at,
			$status,
			$operation_id
		);
	`
}

func CreateBackupQuery() string {
	//skipped completed_at
	return `
		UPSERT INTO Backup (
			id,
			container_id,
			database,
			initiated,
			s3_endpoint,
			s3_region,
			s3_bucket,
			s3_path_prefix,
			status,
			paths
		) VALUES (
			$id,
			$container_id,
			$database,
			$initiated,
			$s3_endpoint,
			$s3_region,
			$s3_bucket,
			$s3_path_prefix,
			$status,
			$paths
		);
	`
}
