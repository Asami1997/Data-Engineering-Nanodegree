from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        

    def execute(self, context):
        self.log.info('Running DataQualityOperator')
        
        redshitf_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #loop through the 
        for table in tables :
            records_count = redshitf_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1 :
                self.error(f"Data Quality Check on Table {table} has Failed")
                raise ValueError(f"Data Quality Check on Table {table} has Failed") 
            #passed    
            self.log(f"Data Quality Check on table {table} has Successed")        