from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    check_dict = {
        'songs': {'table':'songs', 'col':'title', 'expected_nulls': 0},
        'users': {'table':'users', 'col':'last_name', 'expected_nulls': 0},
        'artists': {'table':'artists', 'col':'name', 'expected_nulls': 0},
        'songplay': {'table':'songplays', 'col':'user_id', 'expected_nulls': 0}
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
            records = redshift.get_records(f"SELECT COUNT(*) FROM {value['table']}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {value['table']} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {value['table']} contained 0 rows")
            self.log.info(f"Data quality check on table {value['table']} passed with {records[0][0]} records")
            null_records = redshift.get_records(f"SELECT COUNT(*) FROM {value['table']} where {value['col']} is null")
            num_null_records = null_records[0][0]
            if num_null_records != value['expected_nulls']:
                raise ValueError(f"Data quality check failed. {value['table']} contained {null_records[0][0]} null {value['col']}")
            self.log.info(f"Data quality check on table {value['table']} passed with {num_null_records} null {value['col']}")
