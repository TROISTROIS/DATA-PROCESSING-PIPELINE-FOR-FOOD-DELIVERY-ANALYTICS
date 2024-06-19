from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime


dag = DAG(
    'submit_pyspark_job_to_emr_food_delivery_project',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
)

sensor_key = S3KeySensor(
    task_id="s3_Key_Sensor",
    bucket_name= None,
    bucket_key= "s3://food-delivery-bucket-fn/food_delivery_data/food_delivery_data.csv",
    aws_conn_id = 'aws_default'
)

step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id='j-3NYVZG6AK5JK7',
    aws_conn_id='aws_default',
    steps=[{
        'Name': 'Run PySpark Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                's3://pyspark-scripts-for-projects-fn/project-food-delivery/pyspark-scripts/pyspark_job.py',
            ],
        },
    }],
    dag=dag,
)

step_checker = EmrStepSensor(
    task_id='check_step',
    job_flow_id='j-3NYVZG6AK5JK7',
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,  # Check every 2 minutes
    timeout=86400,  # Fail if not completed in 1 day
    mode='poke',
    dag=dag,
)

sensor_key >> step_adder >> step_checker