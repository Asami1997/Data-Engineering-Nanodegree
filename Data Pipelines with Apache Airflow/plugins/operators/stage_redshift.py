from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    #class variable specifc to redshift
    copy_sql_statement = """
    copy {}
    from {}
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    {}
    """
    
    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 aws_credentials_id,
                 s3_bucket,
                 s3_key,
                 file_format,
                 ignore_header = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket =  s3_bucket
        self.s3_key = s3_key
        self.ignoreheader = ignore_header
        
             
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        # get the redshift hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostregsHook(postregs_conn_id = self.redshift_conn_id)
        
        self.log.info("Clear the data from the redshift table")
        
        redhsift.run("DELETE FROM {}".format(table))
        # get data from the S3 bucket 
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3//" +  self.s3_bucket + '/' + self.s3_key
        
        #handle csv and JSON file formats
        # if json , no need to pass anything
        additional = ""
        if file_format == 'CSV':
            additional = "DELIMITER ',' IGNOREHEADER 1 "
        
        
        formatted_sql_query = StageToRedshiftOperator.copy_sql_statement.format(
         self.table, 
         self.s3_path,
         credentials.access_key,
         credentials.secret_key,
         additional        
        )
        
        #run the query against redshift
        redshift.run(formatted_sql_query)