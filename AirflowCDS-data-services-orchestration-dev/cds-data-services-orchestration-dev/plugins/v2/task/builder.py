from plugins.v2.task.common import DummyTask, EntityTask, DiscoverFile, ParallelGroupEnd
from plugins.v2.task.aws.s3.builder import S3CopyFileTask, S3ChangeMetadataTask, S3DeleteFileTask
from plugins.v2.task.db.postgres.builder import PostgresQueryExecutionTask
from plugins.v2.task.aws.athena.builder import AthenaTableCreateTask, AthenaTableDropTask, AthenaTableRepairTask, Bookmark
from plugins.v2.task.aws.emr.builder import EmrTaskExecutor
from plugins.v2.db.oms_ods_admin import CdsDataProcessingStatusConfiguration, CdsDataProcessingTaskConfiguration
from airflow.utils.task_group import TaskGroup


class CdsDataProcessor(object):
    """CdsDataProcessor: Discovers the subsources for a datasource."""

    table_name = ''
    config_id = ''

    def __init__(self, data=None):
        super(CdsDataProcessor, self).__init__()

        if data is not None:
            self.table_name = data[0]
            self.config_id = data[1]
            self.source_config = data[2]

    @staticmethod
    def discover_subsource(data_source):
        return CdsDataProcessingStatusConfiguration().discover_subsource(data_source)


class TaskFactory(object):
    """TaskFactory: Factory to generate tasks on the fly based on configuration object"""

    def __init__(self, dag):
        self.dag = dag

    TASK_LIST = {
        's3_file_copy': S3CopyFileTask,
        's3_file_change_metadata': S3ChangeMetadataTask,
        's3_file_delete': S3DeleteFileTask,
        'postgres_query_execution': PostgresQueryExecutionTask,
        'athena_table_create': AthenaTableCreateTask,
        'athena_table_drop': AthenaTableDropTask,
        'athena_table_repair': AthenaTableRepairTask,
        'bookmark': Bookmark,
        'discover_file': DiscoverFile,
        'emr_task_executor': EmrTaskExecutor
    }

    @staticmethod
    def generate_tasks(main_conf, configs, dag, index, entity_task_id):
        airflow_tasks = []
        task_idx = 0
        revised_tasks = {}
        for idx, task_conf in enumerate(configs):
            task_type = task_conf['type'].lower()

            task_conf = {**main_conf, **task_conf}

            if task_type not in TaskFactory.TASK_LIST:
                continue

            # Detect if the task is final
            is_final_task = (len(configs) - 1 == idx)

            if task_idx > 0:
                at = airflow_tasks[task_idx - 1]
                if type(at) == list:
                    prev_task_id = at[len(at) - 1].task_id
                else:
                    prev_task_id = at.task_id
            else:
                prev_task_id = None

            task_index = f'{index}_{1 + idx}'
            task = TaskFactory.TASK_LIST[task_type](task_conf, dag).generate(task_index, entity_task_id, is_final_task,
                                                                             prev_task_id)

            if task is not None:

                if 'parallel-group' in task_conf:
                    g = task_conf['parallel-group']

                    if idx not in revised_tasks:
                        revised_tasks.setdefault(task_conf['parallel-group'], []).append(task)

                    if revised_tasks[task_conf['parallel-group']] not in airflow_tasks:
                        airflow_tasks.append(revised_tasks[task_conf['parallel-group']])
                        task_idx += 1
                else:
                    airflow_tasks.append(task)
                    task_idx += 1

        return airflow_tasks


class TaskBuilder(object):
    """TaskBuilder: Sets tasks dependencies and relationships for the subsources"""

    def __init__(self, dag):
        self.dag = dag

    def generate_task_pipeline(self, data_source):

        start = end = None

        sources = CdsDataProcessor.discover_subsource(data_source)

        if sources:
            start = DummyTask({'name': 'start'}, self.dag).generate()
            end = DummyTask({'name': 'end'}, self.dag).generate()

        sections = {}
        for source in sources:
            conf = source[2]
            if 'section' in conf:
                section_seq = conf['section_seq'] if 'section_seq' in conf else 0
                sections[section_seq] = conf['section']

        sections = sorted(sections.items(), key=lambda x: x)

        section_tasks = {}
        table_index = 0
        for source in sources:
            cdp = CdsDataProcessor(source)
            with TaskGroup(group_id=f'{cdp.table_name}', prefix_group_id=False) as task_group:
                table_index += 1

                tasks_config = CdsDataProcessingTaskConfiguration().get_all_tasks(cdp.config_id)
                track_status = source[2].get('track_status', True)

                entity, entity_task_id = EntityTask(
                    {'name': cdp.table_name, 'config_id': cdp.config_id, 'track_status': track_status},
                    self.dag).generate(table_index)

                tasks = TaskFactory.generate_tasks(cdp.source_config, tasks_config, self.dag, table_index,
                                                   entity_task_id)

                if sections and 'section' in source[2]:

                    g_name = source[2]['section']
                    g_sequence = source[2].get('section_seq')
                    next_section = source[2].get('next_section')

                    loader_key = entity_task_id.split('_', 2)[-1]

                    if g_name not in section_tasks:
                        section_tasks[g_name] = []

                    tasks.insert(0, entity)

                    section_tasks[g_name].append(
                        {
                            'tasks': tasks,
                            'table_index': table_index,
                            'task_group': task_group,
                            'section_name': g_name,
                            'sequence_no': g_sequence,
                            'next_section': next_section
                        }
                    )

                    if any(isinstance(i, list) for i in tasks):
                        section_tasks[g_name][-1]['pte'] = ParallelGroupEnd(
                            {
                                'task_id': f'end_group_{table_index}',
                                'op_kwargs': {'xcom_loader_key': loader_key, 'entity-task': entity_task_id},
                            },
                            self.dag
                        ).generate()

                else:
                    tasks.insert(0, start)
                    tasks.insert(1, entity)
                    tasks.append(end)

                    pts = None
                    pte = None
                    loader_key = entity_task_id.split('_', 2)[-1]

                    for idx, task in enumerate(tasks):

                        if idx == 0:
                            continue

                        elif type(task) == list:
                            if pts is None and pte is None:
                                pts = tasks[idx - 1]
                                pte = ParallelGroupEnd(
                                    {
                                        'task_id': f'end_group_{table_index}',
                                        'op_kwargs': {'xcom_loader_key': loader_key, 'entity-task': entity_task_id},
                                    },
                                    self.dag
                                ).generate()

                            for ti, t in enumerate(task):
                                if ti == 0:
                                    pts >> t
                                else:
                                    task[ti - 1] >> t

                            task[-1] >> pte

                        else:
                            if type(tasks[idx - 1]) == list and pte is not None:
                                pte >> task
                            else:
                                tasks[idx - 1] >> task

        if sections:
            task_groups = {}
            for section in sections:

                g_seq, g_name = section

                gt_list = section_tasks[g_name]
                for t_obj in gt_list:
                    tasks = t_obj['tasks']

                    pts = None
                    pte = None

                    for idx, task in enumerate(tasks):
                        if idx == 0:
                            continue

                        elif type(task) == list:
                            if pts is None and pte is None:
                                pts = tasks[idx - 1]
                                pte = t_obj['pte']

                            for ti, t in enumerate(task):
                                if ti == 0:
                                    pts >> t
                                else:
                                    task[ti - 1] >> t

                            task[-1] >> pte

                        else:
                            if type(tasks[idx - 1]) == list and pte is not None:
                                pte >> task
                            else:
                                tasks[idx - 1] >> task

                    g_s_key = t_obj['section_name'] if t_obj['sequence_no'] is None else f"{t_obj['section_name']}-{t_obj['sequence_no']}"

                    task_groups[g_s_key] = task_groups.get(g_s_key, [])

                    task_groups[g_s_key].append(t_obj)

            for s_name in task_groups:

                for t_obj in task_groups[s_name]:

                    if t_obj['sequence_no'] == 1:
                        start >> t_obj['task_group']

                    next_section = t_obj.get('next_section')

                    if next_section and next_section in task_groups:

                        t_obj['task_group'] >> task_groups[next_section][0]['task_group']

                    elif next_section is None:

                        t_obj['task_group'] >> end
