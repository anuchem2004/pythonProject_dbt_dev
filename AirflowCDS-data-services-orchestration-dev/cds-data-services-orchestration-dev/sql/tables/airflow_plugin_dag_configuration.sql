-- DROP TABLE oms_ods_admin.airflow_plugin_dag_configuration;

CREATE TABLE oms_ods_admin.airflow_plugin_dag_configuration (
	config_id bigserial NOT NULL,
	data_source text NOT NULL,
	sub_source text NOT NULL,
	config_details json NULL,
	description text NULL,
	created_dt timestamp NULL DEFAULT now(),
	updated_dt timestamp NULL DEFAULT now(),
	is_active bool NULL,
	CONSTRAINT airflow_plugin_dag_configuration_pkey1 PRIMARY KEY (config_id)
);