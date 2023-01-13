from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ------------ Manager (Model objects handler) ------------ #
class PgDB:
    CONNECTION_ID = 'postgres_dam_loader'
    connection = None
    table_name = None

    COMPARISON_OPERATORS = {
        "$eq": "=",
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
        "$ne": "<>",
        "$in": "IN",
        "$nin": "NOT IN"
    }

    KEYWORD = ['NULL', 'CURRENT_TIMESTAMP']

    def __init__(self):
        if self.table_name is None:
            raise Exception('Table information missing')

        self.set_connection()

    def set_connection(self):      
        schema = BaseHook.get_connection(self.CONNECTION_ID).schema
        pg_hook = PostgresHook(postgres_conn_id=self.CONNECTION_ID, schema=schema)
        connection = pg_hook.get_conn()
        connection.autocommit = True
        self.connection = connection

    def _get_cursor(self):
        return self.connection.cursor()

    def _execute_query(self, query, params=None):
        cursor = self._get_cursor()
        cursor.execute(query, params)

    def _build_sql(self, query, fields=None):
        query_terms = []

        try:
            for column, term in query.items():

                if type(term) is dict:
                    for operator, value in term.items():
                        if operator in self.COMPARISON_OPERATORS:
                            operator = self.COMPARISON_OPERATORS[operator]
                            final_term = "(%s)" % ','.join(["'%s'" % col for col in value]) if type(value) is list else str(value)
                            query_terms.append(f"{column} {operator} {final_term}") if final_term.startswith('(') else query_terms.append(f"{column} {operator} '{final_term}'")
                        else:
                            print(f'Invalid query parameter {operator}')
                else:
                    query_terms.append(f"{column}='{term}'")

            column_lst = fields if fields is not None else None
            columns = ','.join(column_lst) if column_lst else '*'
            sql_stmt = f'SELECT {columns} FROM {self.table_name}'

            if query_terms:
                where_stmt = ' AND '.join(query_terms)
                sql_stmt += f' WHERE {where_stmt}'

        except Exception as e:
            print(e)

        else:
            return sql_stmt

    def select(self, filters={}, fields=None, chunk_size=2000):
        # Build SELECT query
        query = self._build_sql(filters, fields)       

        # Execute query
        cursor = self._get_cursor()
        cursor.execute(query)

        model_objects = []
        is_fetching_completed = False
        while not is_fetching_completed:
            result = cursor.fetchmany(size=chunk_size)
            for row_values in result:
                model_objects.append(row_values)
            is_fetching_completed = len(result) < chunk_size

        return model_objects

    def custom_select(self, query):
        cursor = self._get_cursor()
        cursor.execute(query)
        return cursor.fetchall()      

    def custom_insert(self, query):
        cursor = self._get_cursor()
        cursor.execute(query)
        return cursor.fetchone()

    def bulk_insert(self, rows: list):
        # Build INSERT query and params:
        field_names = rows[0].keys()
        field_values = rows[0].values()
        assert all(row.keys() == field_names for row in rows[1:])  # confirm that all rows have the same fields

        fields_format = ", ".join(field_names)
        fields_values = ", ".join(field_values)
       
        query = f"INSERT INTO {self.table_name} ({fields_format}) " \
                f"VALUES ({fields_values})"

        self._execute_query(query)


    def update(self, where: dict, new_data: dict):                
        # Build UPDATE query and params
        field_names = new_data.items()
        filter_fields = where.items()
        check_key = ''

        for field_name, field_value in field_names:
            if str(field_value).upper() in self.KEYWORD:
                check_key += f", {field_name} = {field_value}"
            else:
                check_key += f", {field_name} = '{field_value}'"

        placeholder_format = check_key.replace(',', '', 1)

        # placeholder_format = ', '.join([f"{field_name} = {field_value}" for field_name, field_value in field_names])
        where_filter = ', '.join([f"{field_name} = '{field_value}'" for field_name, field_value in filter_fields])

        query = f"UPDATE {self.table_name} SET {placeholder_format} WHERE {where_filter}"

        # Execute query
        self._execute_query(query)
        