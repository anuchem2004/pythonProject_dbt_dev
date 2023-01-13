-- DROP TABLE oms_ods_admin.airflow_plugin_task_type;

CREATE TABLE oms_ods_admin.airflow_plugin_task_type (
	task_type_id bigserial NOT NULL,
	task_type_name text NOT NULL,
	description text NULL,
	config_details json NULL,
	created_dt timestamp NULL DEFAULT now(),
	CONSTRAINT airflow_plugin_task_type_pkey1 PRIMARY KEY (task_type_id)
);

-- Supported Task Types

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	1,
	'ATHENA_TABLE_REPAIR',
	'MSCK repair for Athena table',
	'{
		"database": "target database",
		"table": "target table",
		"query_location": "s3 location for query result"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	2,
	'ATHENA_TABLE_CREATE',
	'Create Athena table',
	'{
	"fully_qualified_destination": "source for the table (database.table)",
	"format": "TEXTFILE",
	"fully_qualified_object": "database.view/table",
	"bucketed_by": "unique column to create only one file (optional)",
	"delimeter": "The single-character field delimiter for files in CSV, TSV, and text files",
	"object_location": "location where table data will store "
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	3,
	'ATHENA_TABLE_DROP',
	'Drop Athena table',
	'{
	"database": "target database",
	"table": "target table",
	"query_location": "s3 location for query result"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	4,
	'BOOKMARK',
	'Create bookmark table in athena from source',
	'{
		"fully_qualified_destination": "athena database and table where we want to pupulate bookmark table (ex: test.bookmark)",
		"fully_qualified_source": "Fully qualified source (ex: data_source.database.table )",
		"object_location": "S3 loaction where bookmark table will be created"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	5,
	'S3_FILE_CHANGE_METADATA',
	'Change S3 file metadata',
	'{
	"location": "file location",
	"pattern": "target pattern (ex: gz)"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	6,
	'S3_FILE_COPY',
	'Copy S3 file from source to destination',
	'{
	"source_bucket": "source bucket name",
	"source_location": "source bucket key",
	"target_bucket": "target bucket name",
	"target_location": "target bucket location",
	"source_pattern": "source file pattern"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	7,
	'S3_FILE_DELETE',
	'Delete file from provided S3 location',
	'{
	"location": "target files key",
	"pattern": "target files pattern",
	"delete_all": "true(to delete all file)/false(to delete only one file)"
	}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	8,
	'POSTGRES_QUERY_EXECUTION',
	'Execute PostgreSQL query',
	'{
	"query": "sql query or sproc to execute (ex: CALL schema.sproc_name({dynamic param in curly braces}), ex: Update schema.table_name set field_name = value where filter_fields )"
}'::json
);

insert
	into
	oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name,
	description,
	config_details
)
values
(
	9,
	'DISCOVER_FILE',
	'Discover unprocessed control files',
	'{
	"file_type": "CHANGE",
	"parallel-group": "parallel group name",
	"pattern": "control file pattern",
	"entity_type": "entity name",
	"bucket": "temporary control file bucket",
	"key": "temporary control file location"
	}'::json
);

INSERT INTO oms_ods_admin.airflow_plugin_task_type
(
	task_type_id,
	task_type_name, 
	description, 
	config_details
)
VALUES
(
	10,
	'EMR_TASK_EXECUTOR', 
	'Submit spark job to EMR and wait till job completion', 
	'{
	"emr_task_name": "base_extractor",
	"emr_job_name": "ehub_extractor",
	"app_path": "s3://my_bucket/jar_file_location/jar_file.jar",
	"cluster_id": "cluster_id",
	"step_args": {
		"--name": "emr_extractor",
		"deploy-mode": "cluster",
		"class": "class.name",
		"processor": "Extractor",
		"config": "config_details"
		}
	}'::json
);