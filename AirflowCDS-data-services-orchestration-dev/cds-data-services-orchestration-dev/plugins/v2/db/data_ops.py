from plugins.v2.db.common import PgDB
        
SCHEMA = 'data_ops'

class TestCases(PgDB):
    """ test_cases table columns 
        - id
        - test_case_type_id
        - test_case_config
        - test_case_result_config
        - description
        - created_dt
        - updated_dt
        - status
    """
    table_name = '%s.test_cases' % SCHEMA

    def __init__(self):
        super(TestCases, self).__init__()

    def get_active_test_cases(self):

        query = f'''
            SELECT
                tc.id
            FROM
                {self.table_name} tc
                join data_ops.test_case_type tct
                    on tc.test_case_type_id = tct.id
            WHERE
                tc.status = 1 and tct.isactive = true
        '''
        test_cases = self.custom_select(query)

        return test_cases
    
    def get_pending_test_cases(self): 
         
        query = f''' 
        select
            tc.id test_case_id,
            tcl.id test_case_log_id,
            tc.test_case_config ->> 's3_location' source_location,
            tc.test_case_result_config ->>'s3_location' output_location,
            tc.description test_case_description
        from
            {self.table_name} tc
        join data_ops.test_case_logs tcl 
        on
            tcl.test_case_id = tc.id
        where
            tcl.test_case_execution_status = 1 
        ''' 

        test_cases= self.custom_select(query)

        tests = []
        for d in test_cases:
            tests.append({  
                'test_case_id' :  d[0],
                'test_case_log_id': d[1],
                'sql_s3_location': d[2],
                'out_s3_location': d[3],
                'test_case_description' : d[4],
                })

        return tests


class TestCaseLog(PgDB):
    """ test_case_logs table columns
        - id
        - test_case_id
        - test_case_config
        - test_case_result_config
        - description
        - created_dt
        - updated_dt
        - test_case_execution_status
        - test_case_result_status
    """
    table_name = '%s.test_case_logs' % SCHEMA

    def __init__(self):
        super(TestCaseLog, self).__init__()

    def create(self, test_case_id):
        """
        Create new row in test case log
        Parameters
        ----------
        test_case_id:
            test case id from test case table to create a new log record
        """
        field = [
            {
                "test_case_id": str(test_case_id),
                "created_dt": "now()",
                "updated_dt": "now()",
                "test_case_execution_status": "1"
            }
        ]
        self.bulk_insert(field)

    def update_results(self, update_id, fields): 
        """
        Update result details in specified row id
        Parameters
        ----------
        update_id:
            test_case_logs.id
        fields:
            dictionary contain table columns with values to update
        """      
        filter = {
            "id": update_id
        }

        self.update(filter, fields)
    