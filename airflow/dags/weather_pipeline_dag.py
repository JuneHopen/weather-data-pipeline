from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
	'owner': 'Avesina',
	'start_date': days_ago(1),
	'email': 'avesinaroszah@gmail.com',
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
	'catchup':False,
}

dag = DAG(
	'Weather_pipeline',
	default_args=default_args,
	description='Weather pipeline schedule',
	schedule_interval='@hourly',
)

producer = BashOperator(
	task_id='producer_run',
	bash_command='python3 weather_producer.py',
	cwd='/Users/admin/project/weather_data_pipeline/producer/',
	dag=dag,
)

consumer = BashOperator(
	task_id='consumer_run',
	bash_command='python3 weather_consumer.py',
	cwd='/Users/admin/project/weather_data_pipeline/consumer/',
	dag=dag,
)

producer >> consumer
