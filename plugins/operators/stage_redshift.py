from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # Set fields on which templates applied
    template_fields = ("s3_key",)
    # Copy query from S3 to redshift for json files
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 redshift_table="",
                 s3_bucket="",
                 s3_key="",
                 jsonpath="",
                 *args, **kwargs):
        """
        Initializes StageToRedshiftOperator
        
        :param redshift_conn_id: reference to redshift connection info
        :type redshift_conn_id: str
        :param aws_credentials_id: reference to aws credential info
        :type aws_credentials_id: str
        :param redshift_table: redshift table name for staging S3 data into
        :type redshift_table: str
        :param s3_bucket: S3 bucket name
        :type s3_bucket: str
        :param s3_key: S3 path (with airflow templates) where raw data is located
        :type s3_key: str
        :param jsonpath: Path to json format
        :type jsonpath: str
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_table = redshift_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonpath = jsonpath

    def execute(self, context):
        """
        Executes staging(S3 to redshift) work
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Copying data from S3 to Redshift table [{self.redshift_table}]")
        self.log.info(f"S3 Key value is [{self.s3_key}]")
        rendered_key = self.s3_key
        s3_path="s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"S3 path for staging is {s3_path}")
        formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
            self.redshift_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.jsonpath
        )
        
        redshift_hook.run(formatted_sql)
            





