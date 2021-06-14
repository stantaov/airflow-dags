# from datetime import datetime as dt
# from datetime import timedelta
# from airflow.utils.dates import days_ago
# #The DAG object; we'll need this to instantiate a DAG
# from airflow import DAG
# #importing the operators required
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# #these args will get passed to each operator
# #these can be overridden on a per-task basis during operator #initialization
# #notice the start_date is any date in the past to be able to run it #as soon as it's created
# default_args = {
# 'owner' : 'airflow',
# 'depends_on_past' : False,
# 'start_date' : days_ago(2),
# 'email' : ['example@123.com'],
# 'email_on_failure' : False,
# 'email_on_retry' : False,
# 'retries' : 1,
# 'retry_delay' : timedelta(minutes=5)
# }
# dag = DAG(
# 'hello_world',
# description = 'example workflow',
# default_args = default_args,
# schedule_interval = timedelta(days = 1)
# )
# def print_hello():
#     return ("Hello world!")
# #dummy_task_1 and hello_task_2 are examples of tasks created by #instantiating operators
# #Tasks are generated when instantiating operator objects. An object #instantiated from an operator is called a constructor. The first #argument task_id acts as a unique identifier for the task.
# #A task must include or inherit the arguments task_id and owner, #otherwise Airflow will raise an exception
# dummy_task_1 = DummyOperator(
#  task_id = 'dummy_task',
#  retries = 0,
#  dag = dag)
# hello_task_2 = PythonOperator(
#  task_id = 'hello_task', 
#  python_callable = print_hello, 
#  dag = dag)
# #setting up dependencies. hello_task_2 will run after the successful #run of dummy_task_1
# dummy_task_1 >> hello_task_2


import os
import boto3
import json
import requests
import time
import logging
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow import AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


DEFAULT_ARGS = {
    'owner': 'Stan Taov',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1), # enable event driven approach
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

# Checks if the EMR cluster is up and running
class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)
    # poke function will be called and checks that the clsuter status is WAITING
    def poke(self, context):
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            logging.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            logging.info(e)
            return False


# Retrives an instance region to use it later for creating boto3 clients
def get_region():
    instance_info = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    instance_info_dict = instance_info.json()
    return instance_info_dict.get('region') 

# Creates a boto3 ERM client 
def emr_clent(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)

# Creates an EMR cluster
def create_cluster(region_name, cluster_name='Spark-Cluster'):
    cluster = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel='emr-5.33.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master Node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': "Worker Node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 3
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'midterm',
            'EmrManagedMasterSecurityGroup': 'sg-0916a76e1f13d70c9',
            'EmrManagedSlaveSecurityGroup': 'sg-095446c5e365749e1',
        },
		# adding HIVE and Spark metastores to regester them later with Glue Data Catalog
		Configurations=[{
			'Classification': 'hive-site',
			'Properties': {
				'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
			}
		}, 
			{
			'Classification': 'spark-hive-site',
			'Properties': {
				'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
			}
		}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            { 'Name': 'Hadoop'},
            { 'Name': 'Hive'},
            { 'Name': 'Spark'}
        ]
    )
    return cluster['JobFlowId']

# Gets the EMR current cluster status
def get_cluster_status(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

# Terminates the EMR cluster
def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])


# # Tetrives location of the uploaded file on S3
# def retrieve_s3_file(**kwargs):
#     s3_location = kwargs['dag_run'].conf['s3_location']
#     kwargs['ti'].xcom_push(key = 's3_location', value = s3_location)


# # Creates an EMR cluster
# def create_emr(**kwargs):
#     cluster_id = emr.create_cluster(region_name=region)
#     Variable.set("cluster_id", cluster_id)
#     return cluster_id

# # Terminates the EMR cluster
# def terminate_emr(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     emr.terminate_cluster(cluster_id)
#     # Sets Airflow Variable key for cluster_id to na
#     Variable.set("cluster_id", "na")

# # ELT job complete pointer
# def dag_done(**kwargs):
#     Variable.set("dag_emr_job", "done")

# # Runs predefined Glue crawler 
# def cwarler_run(**kwargs):
#     client = boto3.client('glue', region_name=region)
#     response = client.start_crawler(
#     Name='data_enginnering_midterm_project'
#     )

# # Repairs Athena partitions
# def reapir_table(**kwargs):
#     client = boto3.client('athena', region_name=region)
#     queryStart = client.start_query_execution(
#         QueryString = "MSCK REPAIR TABLE output",
#         QueryExecutionContext = {
#         'Database': 'data_enginnering_midterm_project'
#         }, 
#         ResultConfiguration = { 'OutputLocation': 's3://midterm-project-wcd/output_data/'}
#     )

# def file_extension_checker():
#     s3 = boto3.resource('s3')
#     bucket = s3.Bucket('midterm-project-wcd')
#     files = bucket.objects.all()
#     extensions = ['csv', 'json']
#     for file in files:
#         if file.key[-3:] in extensions:
#             return file.key[-3:].capitalize()


# region = get_region()
# cluster = emr_clent(region_name=region)
# extension = file_extension_checker()

# SPARK_STEPS = [
#     {
#         'Name': 'spark-job',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': [
#                 '/usr/bin/spark-submit', 
#                 '--class', 'Driver.MainApp',
#                 '--master', 'yarn',
#                 '--deploy-mode','cluster',
#                 '--num-executors','2',
#                 '--driver-memory','1g',
#                 '--executor-memory','3g',
#                 '--executor-cores','2',
#                 '--conf', 'spark.shuffle.service.enabled=true',
#                 '--conf', 'spark.dynamicAllocation.enabled=true',
#                 '--conf', 'spark.dynamicAllocation.minExecutors=2',
#                 '--conf', 'spark.dynamicAllocation.maxExecutors=10',
#                 '--conf', 'spark.dynamicAllocation.initialExecutors=2',
#                 's3a://midterm-project-wcd/spark-job/wcd_final_project_2.11-0.1.jar',
#                 '-p','midterm-project',
#                 '-i','extension', 
#                 '-o','parquet',
#                 #'-s','s3a://midterm-project-wcd/input_data/banking.csv',
#                 '-s', "{{ task_instance.xcom_pull('parse_request', key='s3_location') }}",
#                 '-d','s3a://midterm-project-wcd/output_data/',
#                 '-c','job', # come up with a solution to pick partition automatically
#                 '-m','append',
#                 '--input-options','header=true'
#             ]
#         }
#     }
# ]


# Creating DAG workflow 
dag = DAG(
    'emr_spark_job',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=['emr']
)

# create_cluster = PythonOperator(
#     task_id='create_cluster',
#     python_callable=create_emr,
#     dag=dag)

# emr_cluster_check = ClusterCheckSensor(
#     task_id='cluster_check', 
#     poke_interval=60, 
#     dag=dag)

# parse_request = PythonOperator(
#     task_id='parse_request',
#     provide_context=True,
#     python_callable=retrieve_s3_file,
#     dag=dag
# )

# step_adder = EmrAddStepsOperator(
#     task_id='add_steps',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
#     aws_conn_id='aws_default',
#     steps=SPARK_STEPS,
#     dag=dag
# )

# step_checker = EmrStepSensor(
#     task_id='watch_step',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
#     step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
#     aws_conn_id='aws_default',
#     dag=dag
# )

# crawler = PythonOperator(
#     task_id = 'glue_crawler',
#     python_callable=cwarler_run,
#     dag=dag
# )

# repair = PythonOperator(
#     task_id = 'rapair_athena_table',
#     python_callable=reapir_table,
#     dag=dag
# )

# job_complete = PythonOperator(
#     task_id = "ETL_job_complete",
#     python_callable=dag_done,
#     dag=dag
# )

# terminate_cluster = PythonOperator(
#     task_id='terminate_cluster',
#     python_callable=terminate_emr,
#     trigger_rule='all_success', # make sure that all tasks were completed successfully
#     dag=dag)

# # Setting the Airflow workflow
# parse_request >> create_cluster
# create_cluster >> emr_cluster_check
# emr_cluster_check >> step_adder
# step_adder >> step_checker
# step_checker >> crawler
# crawler >> repair
# repair >> job_complete
# job_complete >> terminate_cluster
