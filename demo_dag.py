  
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator

yesterday = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': yesterday,
    'email': 'piotr.hyzy@atos.net',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def demo(argument, **context):
    print(argument)
    print('input parameters:')
    print(context['dag_run'].conf)

with DAG('demo_dag', schedule_interval=None,
         default_args=default_args) as dag:

    wait_task = GoogleCloudStoragePrefixSensor(
        task_id='filesensor',
        bucket='{{var.value.gcs_bucket}}',
        prefix='{{var.value.gcs_file}}',
        google_cloud_conn_id='google_cloud_default',
        dag=dag
    )

    transform_file = GCSFileTransformOperator(
        task_id="transform_file",
        source_bucket='{{var.value.gcs_bucket}}',
        source_object='devfest',
        destination_bucket='{{var.value.gcs_bucket}}',
        destination_object='new_file',
        transform_script=["cp"],
        google_cloud_conn_id='google_cloud_default',
    )

    wait_task >> transform_file 

    for i in 1, 2:
    
        transform_file >> PythonOperator(
            task_id='hello_world_' + str(i),
            provide_context=True,
            python_callable=demo,
            op_kwargs = { 
                "argument": 'hello world',
            }
            )


    