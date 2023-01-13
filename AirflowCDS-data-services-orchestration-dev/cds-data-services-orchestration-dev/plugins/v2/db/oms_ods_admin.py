import json
from plugins.v2.db.common import PgDB

SCHEMA = 'oms_ods_admin'


class CdsDataProcessingStatusConfiguration(PgDB):
    """ airflow_plugin_dag_configuration table columns
        - config_id
        - data_source
        - sub_source
        - config_details
        - description
        - created_dt
        - updated_dt
        - is_active
    """
    table_name = f'{SCHEMA}.airflow_plugin_dag_configuration'

    def __init__(self):
        super(CdsDataProcessingStatusConfiguration, self).__init__()

    def discover_tables(self, data_source):
        """
        Get list of all active tables for given matching data source
        Returns
        -------
        list of matching configuration objects 
        """
        if data_source is None:
            raise Exception('data_source information missing')

        where = {
            'data_source': {
                '$eq': data_source
            },
            'is_active': {
                '$eq': 1
            }
        }

        fields = [
            "sub_source",
            "config_details ->> 'sourceS3Bucket'",
            "config_details ->> 'athenaSourceDb'",
            "config_details ->> 'athenaSourceTable'",
            "config_details ->> 'athenaQueryLocation'",
            "config_details ->> 'athenaTargetDb'",
            "config_details ->> 'athenaTargetTable'",
            "config_details ->> 'athenaExportLocation'",
            "config_details ->> 'sprocName'",
            "config_id",
            "config_details ->> 'athenaSourceDbFact'",
            "config_details ->> 'factsTable'",
            "config_details ->> 'sequential'",
            "config_details ->> 'cdsOdsTable'",
            "config_details ->> 'cdsOdsSchema'"
        ]

        return self.select(where, fields)

    def discover_subsource(self, data_source):
        """
        Get list of all active tables for given matching data source
        Returns
        -------
        list of matching configuration objects 
        """
        if data_source is None:
            raise Exception('data_source information missing')

        where = {
            'data_source': {
                '$eq': data_source
            },
            'is_active': {
                '$eq': 1
            }
        }

        fields = [
            "sub_source",
            "config_id",
            "config_details"
        ]

        return self.select(where, fields)


class CdsDataProcessingStatus(PgDB):
    """ cds_data_processing_status table columns
        - processing_id
        - config_id
        - source_details
        - target_details
        - status
        - message
        - created_dt
        - updated_dt
    """

    table_name = f'{SCHEMA}.cds_data_processing_status'

    def __init__(self):
        super(CdsDataProcessingStatus, self).__init__()

    def mark_as_completed(self, processing_id):
        if not processing_id:
            return
        """
        Update cds data processing status to 'Completed' for give processing id 
        Parameters
        ----------
        processing_id:
            processing id row to update
        """
        field = {
            "status": "Completed",
            "message": "NULL",
            "updated_dt": "CURRENT_TIMESTAMP"
        }

        where = {
            "processing_id": processing_id
        }
        self.update(where, field)

    def mark_as_failed(self, processing_id, msg):
        if not processing_id:
            return
        """
        Update cds data processing status to 'Failed' and failure details message for give processing id
        Parameters
        ----------
        processing_id:
            processing id row to update
        msg:
            failure details message
        """
        field = {
            "status": "Failed",
            "message": msg,
            "updated_dt": "CURRENT_TIMESTAMP"
        }

        where = {
            "processing_id": processing_id
        }
        self.update(where, field)

    def mark_as_skipped(self, processing_id, msg):
        if not processing_id:
            return
        """
        Update cds data processing status to 'Skipped' and skip details message for give processing id
        Parameters
        ----------
        processing_id:
            processing id row to update
        msg:
            skipped details message
        """
        field = {
            "status": "Skipped",
            "message": msg,
            "updated_dt": "CURRENT_TIMESTAMP"
        }

        where = {
            "processing_id": processing_id
        }
        self.update(where, field)


class CdsDataProcessingTaskConfiguration(PgDB):
    """docstring for CdsDataProcessingTaskConfiguration"""

    table_name = f'{SCHEMA}.airflow_plugin_task_configuration'

    def __init__(self):
        super(CdsDataProcessingTaskConfiguration, self).__init__()

    def get_all_tasks(self, config_id):

        query = f''' 
                select
                    ms.task_type_name as type,
                    tc.config_details
                from
                    {self.table_name} tc
                join oms_ods_admin.airflow_plugin_task_type ms on
                    tc.task_type_id = ms.task_type_id 
                where data_processing_config_id = {config_id}
                order by tc.task_sequence
        '''

        tasks = self.custom_select(query)
        task_configs = []

        for task in tasks:
            config = json.loads(json.dumps(task[1]))
            config['config_id'] = config_id
            config['type'] = task[0]
            task_configs.append(config)

        return task_configs
