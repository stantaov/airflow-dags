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
            status = get_cluster_status(cluster_id)
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
def client(region_name):
    return boto3.client('emr', region_name=region_name)

def emr_clent(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

region = get_region()
emr = client(region)


# Creates an EMR cluster
def create_cluster(region_name, cluster_name='Spark-Cluster'):
    cluster = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel='emr-6.3.0',
        LogUri='s3://midterm-project-wcd/emr-logs/',
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


# Retrives location of the uploaded file on S3
def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key = 's3_location', value = s3_location)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = create_cluster(region_name=region)
    Variable.set("cluster_id", cluster_id)
    return cluster_id

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_emr')
    terminate_cluster(cluster_id)
    # Sets Airflow Variable key for cluster_id to na
    Variable.set("cluster_id", "na")

# ELT job complete pointer
def dag_done(**kwargs):
    Variable.set("dag_emr_job", "done")

# Runs predefined Glue crawler 
def cwarler_run(**kwargs):
    client = boto3.client('glue', region_name=region)
    response = client.start_crawler(
    Name='data_enginnering_midterm_project'
    )

# Repairs Athena partitions
def reapir_table(**kwargs):
    client = boto3.client('athena', region_name=region)
    queryStart = client.start_query_execution(
        QueryString = "MSCK REPAIR TABLE " + str(Variable.get("file_name")),
        QueryExecutionContext = {
        'Database': 'data_enginnering_midterm_project'
        }, 
        ResultConfiguration = { 'OutputLocation': 's3://midterm-project-wcd/output_data/'}
    )

# Retrives filename and file extension of the uploaded file
def file_extension_checker(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    base=os.path.basename(s3_location)
    file_name, extension = os.path.splitext(base)
    extension = extension[1:]
    Variable.set("extension", extension.capitalize())
    Variable.set("file_name", file_name)
    return extension.capitalize()


region = get_region()
cluster = emr_clent(region_name=region)
# extension = file_extension_checker()

SPARK_STEPS = [
    {
        'Name': 'spark-job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','1g',
                '--executor-memory','3g',
                '--executor-cores','2',
                '--conf', 'spark.shuffle.service.enabled=true',
                '--conf', 'spark.dynamicAllocation.enabled=true',
                '--conf', 'spark.dynamicAllocation.minExecutors=2',
                '--conf', 'spark.dynamicAllocation.maxExecutors=10',
                '--conf', 'spark.dynamicAllocation.initialExecutors=2',
                's3a://midterm-project-wcd/spark-job/spark-engine_2.12-0.0.1-spark3.jar',
                '-p','midterm-project',
                '-i', str(Variable.get("extension", default_var=0)), 
                '-o','parquet',
                #'-s','s3a://midterm-project-wcd/input_data/banking.csv',
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3_location') }}",
                '-d','s3a://midterm-project-wcd/output_data/' + str(Variable.get("file_name", default_var=0)) + '/',
                '-c','job', # come up with a solution to pick partition automatically
                '-m','append',
                '--input-options','header=true, inferSchema=true' # header=true, inferSchema=true
            ]
        }
    }
]


# Creating DAG workflow 
dag = DAG(
    'emr_spark_job',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=['emr']
)

get_extension = PythonOperator(
    task_id='get_extension',
    python_callable=file_extension_checker,
    dag=dag)


create_EMR_cluster = PythonOperator(
    task_id='create_emr',
    python_callable=create_emr,
    dag=dag)

emr_cluster_check = ClusterCheckSensor(
    task_id='cluster_check', 
    poke_interval=60, 
    dag=dag)

parse_request = PythonOperator(
    task_id='parse_request',
    provide_context=True,
    python_callable=retrieve_s3_file,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

crawler = PythonOperator(
    task_id = 'glue_crawler',
    python_callable=cwarler_run,
    dag=dag
)

repair = PythonOperator(
    task_id = 'rapair_athena_table',
    python_callable=reapir_table,
    dag=dag
)

job_complete = PythonOperator(
    task_id = "ETL_job_complete",
    python_callable=dag_done,
    dag=dag
)

remove_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_success', # make sure that all tasks were completed successfully
    dag=dag)

# Setting the Airflow workflow
get_extension >> parse_request
parse_request >> create_EMR_cluster
create_EMR_cluster >> emr_cluster_check
emr_cluster_check >> step_adder
step_adder >> step_checker
step_checker >> crawler
crawler >> repair
repair >> job_complete
job_complete >> remove_cluster
