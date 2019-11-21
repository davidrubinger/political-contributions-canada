from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class CheckHasRowsOperator(BaseOperator):
    """
    Checks that a Postgres table has rows
    
    :param postgres_conn_id: reference to a specific Postgres database
    :type postgres_conn_id: str
    :param postgres_table_name: name of Postgres table to check
    :type postgres_table_name: str
    """
    
    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 postgres_table_name,
                 *args, **kwargs):
        
        super(CheckHasRowsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_table_name = postgres_table_name
    
    def execute(self, context):
        
        postgres_hook = PostgresHook(self.postgres_conn_id)
        n_rows = postgres_hook.get_records(
            f"select count(*) from {self.postgres_table_name};"
        )[0][0]
        
        # Raise ValueError if table has no rows
        if n_rows == 0:
            raise ValueError(
                f"""
                Has rows check failed:
                No rows in {self.postgres_table_name} table
                """
            )
        else:
            self.log.info(
                f"""
                Has rows check passed:
                {self.postgres_table_name} table contains {n_rows} rows
                """
            )
