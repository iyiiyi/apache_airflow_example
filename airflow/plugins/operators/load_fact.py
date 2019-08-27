from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 load_query="",
                 *args, **kwargs):
        """
        Initializes LoadFactOperator
        
        :param redshift_conn_id: reference to redshift connection info
        :type redshift_conn_id: str
        :param fact_table: redshift table name for placing fact data
        :type fact_table: str
        :param load_query: query for building fact data
        :type load_query: str
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.load_query = load_query

    def execute(self, context):
        """
        Builds fact data using staging data
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into fact table [{}]".format(self.fact_table))
        redshift_hook.run(self.load_query)

