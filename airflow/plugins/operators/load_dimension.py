from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 select_sql,
                 mode='append',
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.select_sql = select_sql
        self.mode = mode
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        
        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info('Deleting has been completed.')
            
        query = """
        INSERT INTO {table}
        {select_sql};
        """.format(table = self.table,
                   select_sql = self.select_sql)
        self.log.info(f'Loading data into {self.table} fimension table...')
        redshift_hook.run(query)
        self.log.info('Loading has been completed.')