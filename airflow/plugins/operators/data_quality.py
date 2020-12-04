from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_table=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.check_table = check_table

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        
        
        for table in self.check_table:
            self.log.info(f'Redshift are checking the table called {table} with data quality')
            
            result = redshift_hook.get_records(f"SELECT COUNT (*) FROM {table}")
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"The check has failed. {table} returned no results")
            num_result = result[0][0]
            if num_result < 1:
                raise ValueError(f"The check has failed. {table} contained 0 rows")
                        
            self.log.info(f"Data quality on {table} check passed with {result[0][0]} results")