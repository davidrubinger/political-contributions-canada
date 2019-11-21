from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

from warnings import warn

class CheckFutureYearsOperator(BaseOperator):
    """
    Checks number of observations in Postgres table with a year in the future
    
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
        
        super(CheckFutureYearsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_table_name = postgres_table_name
    
    def execute(self, context):
        
        postgres_hook = PostgresHook(self.postgres_conn_id)
        n_future_years = postgres_hook.get_records(
            f"""
            select count(*)
            from {self.postgres_table_name}
            where year > date_part('year', current_date);
            """
        )[0][0]
        
        # Output warning if there are observations with future years
        if n_future_years > 0:
            warn(
                f"""
                There are {n_future_years} observations in {self.postgres_table_name} table with year in the future
                """
            )
