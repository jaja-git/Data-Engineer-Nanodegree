from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.helpers.sql_queries import SqlQueries


class DataQualityOperator(BaseOperator):

    check_dict = {
        'songs': {'table': 'songs', 'col': 'title'},
        'users': {'table': 'users', 'col': 'last_name'},
        'artists': {'table': 'artists', 'col': 'name'},
        'songplay': {'table': 'songplays', 'col': 'user_id'}
        }

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 checks=check_dict,
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.checks = checks


    def execute(self, context, *args, **kwargs):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for key, value in self.checks.items():
            records = redshift.get_records(f"SELECT COUNT(*) FROM {value['table']}") #[(604,)]
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {value['table']} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {value['table']} contained 0 rows")
            self.log.info(f"Data quality check on table {value['table']} passed with {records[0][0]} records")
            null_records = redshift.get_records(f"SELECT COUNT(*) FROM {value['table']} where {value['col']} is null")
            num_null_records = null_records[0][0]
            if num_null_records != 0:
                raise ValueError(f"Data quality check failed. {value['table']} contained {null_records[0][0]} null {value['col']}")
            self.log.info(f"Data quality check on table {value['table']} passed with {num_null_records} null {value['col']}")
