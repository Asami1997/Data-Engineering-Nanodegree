from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table 
        self.sql_query = sql_query
        

    def execute(self, context):
        self.log.info('Loadding dimension table {}'.format(self.table))
        
        #define the hook
        redshift_hook = PostregsHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(self.sql_query)