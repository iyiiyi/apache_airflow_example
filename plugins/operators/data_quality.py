from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Custom Operator class for checking quality of data.
    Basically, this operator supports 4 types of quality check
    """
    ui_color = '#89DA59'
    # Test case where query must return 0 count
    ZERO      = 'ZERO'
    # Test case where query must return a non-zero count
    NON_ZERO  = 'NON_ZERO'
    # Test case where query must return an empty result
    EMPTY     = 'EMPTY'
    # Test case where query must return a non-empty result
    NON_EMPTY = 'NON_EMPTY'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_list=[],
                 pass_result_list=[],
                 *args, **kwargs):
        """
        Initializes DataQualityOperator
        
        :param redshift_conn_id: reference to redshift connection info
        :type redshift_conn_id: str
        :param query_list: data queries
        :type query_list: list
        :param pass_result_list: results expected for passing tests
        :type pass_result_list: list
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_list = query_list
        self.pass_result_list = pass_result_list

    def execute(self, context):
        """
        Executes actual data quality checks
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Starting DataQualityOperator')
        list_cnt = len(self.query_list)
        for idx in range(list_cnt):
            query       = self.query_list[idx]
            pass_result = self.pass_result_list[idx]
            record = redshift_hook.get_first(query)
            if pass_result == DataQualityOperator.EMPTY:
                if len(record) > 0:
                    raise ValueError("{} test failed for query [{}]".format(DataQualityOperator.EMPTY, query))
                else:
                    self.log.info("{} test passed for query [{}]".format(DataQualityOperator.EMPTY, query))
            elif pass_result == DataQualityOperator.NON_EMPTY:
                if len(record) == 0:
                    raise ValueError("{} test failed for query [{}]".format(DataQualityOperator.NON_EMPTY, query))
                else:
                    self.log.info("{} test passed for query [{}]".format(DataQualityOperator.NON_EMPTY, query))
            elif pass_result == DataQualityOperator.NON_ZERO:
                count = record[0]
                if count == 0:
                    raise ValueError("{} test failed for query [{}]".format(DataQualityOperator.NON_ZERO, query))
                else:
                    self.log.info("{} test passed for query [{}]".format(DataQualityOperator.NON_ZERO, query))
            elif pass_result == DataQualityOperator.ZERO:
                count = record[0]
                if count == 0:
                    self.log.info("{} test passed for query [{}]".format(DataQualityOperator.ZERO, query))
                else:
                    raise ValueError("{} test failed for query [{}]".format(DataQualityOperator.ZERO, query))
                 
        self.log.info('Finishing DataQualityOperation')
        

 

