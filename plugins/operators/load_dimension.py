from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    TRUNCATE_QUERY = 'TRUNCATE {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_table="",
                 load_query="",
                 truncate_yn=False,
                 *args, **kwargs):
        """
        Initializes LoadDimensionOperator
        
        :param redshift_conn_id: reference to redshift connection info
        :type redshift_conn_id: str
        :param dimension_table: redshift table name for placing dimension data
        :type dimension_table: str
        :param load_query: query for building dimension data
        :type load_query: str
        :param truncate_yn: A flag for trunicate, if True: dimension table truncated before insertion
        :type truncate_yn: boolean
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table = dimension_table
        self.load_query = load_query        
        self.truncate_yn = truncate_yn

    def execute(self, context):
        """
        Builds dimension data using staging data
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_yn == True:
            truncateQuery = LoadDimensionOperator.TRUNCATE_QUERY.format(self.dimension_table)
            self.log.info("Truncating data from dimension table [{}]".format(self.dimension_table))
            redshift_hook.run(truncateQuery)
        self.log.info("Loading data into dimension table [{}]".format(self.dimension_table))
        redshift_hook.run(self.load_query)
