-- DROP TABLE oms_ods_admin.airflow_plugin_task_configuration;

CREATE TABLE oms_ods_admin.airflow_plugin_task_configuration (
	task_id bigserial NOT NULL,
	task_type_id int4 NOT NULL,
	data_processing_config_id int4 NOT NULL,
	task_sequence int4 NOT NULL,
	config_details json NULL,
	description text NULL,
	created_dt timestamp NULL DEFAULT now(),
	CONSTRAINT airflow_plugin_task_configuration_pkey1 PRIMARY KEY (task_id)
);