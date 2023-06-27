from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
import os


def get_emr_operators(entity_name, process, instance_count=1):
    bucket_name = os.environ['EMR_BUCKET_NAME']
    SPARK_STEPS = [
        {
            'Name': f'{process}_{entity_name}',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--deploy-mode',
                    'cluster',
                    '--master',
                    'yarn',
                    '--conf',
                    'spark.yarn.submit.waitAppCompletion=true',
                    '--conf',
                    'spark.sql.parquet.fs.optimized.committer.optimization-enabled=true',
                    '--conf',
                    f'spark.yarn.appMasterEnv.EMR_BUCKET_NAME={bucket_name}',
                    '--conf',
                    'spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                    "--jars",
                    f"s3://{bucket_name}/jars/postgresql-42.2.24.jar",
                    '--py-files',
                    f's3://{bucket_name}/packages/packages.zip',
                    f's3a://{bucket_name}/src/job_template.py',
                    entity_name,
                    process,
                    bucket_name
                ],
            },
        }
    ]

    JOB_FLOW_OVERRIDES = {
        'Name': 'airflow-dag-star_schema',
        'ReleaseLabel': 'emr-6.11.0',
        'LogUri': f"s3://{bucket_name}/log/",
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master node',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Worker nodes',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': instance_count,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'Steps': SPARK_STEPS,
        'JobFlowRole': 'LabInstanceProfile',
        'ServiceRole': 'EMR_DefaultRole',
        'BootstrapActions': [
            {
                'Name': 'install_packages',
                'ScriptBootstrapAction': {
                    'Path': f's3://{bucket_name}/bootstrap_actions/install_packages.sh',
                }
            },
        ]
    }

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id=f'job_flow-{entity_name}-{process}',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    job_sensor = EmrJobFlowSensor(
        task_id=f'check_job-{entity_name}-{process}',
        job_flow_id=job_flow_creator.output,
        aws_conn_id='aws_default',
    )
    return job_flow_creator, job_sensor


with DAG(
        "nyc_trip_data",
        default_args={
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="A dag that generates the 'NYC TRIP DATA' Data Product",
        schedule_interval=None,
        start_date=datetime(2022, 8, 1),
        catchup=False,
        tags=["NYC-TRIP-DATA", "EMR"],
) as dag:
    t1, t2 = get_emr_operators('yellow', 'extract', 3)
    t3, t4 = get_emr_operators('green', 'extract')
    t5, t6 = get_emr_operators('yellow', 'transform', 3)
    t7, t8 = get_emr_operators('green', 'transform')
    t9, t10 = get_emr_operators('trip_data', 'transform', 3)
    t11, t12 = get_emr_operators('yellow', 'quality_check', 3)
    t13, t14 = get_emr_operators('green', 'quality_check')

    start = EmptyOperator(
        task_id="start"
    )

    start >> t1 >> t2 >> t5 >> t6 >> t11 >> t12 >> t9 >> t10
    start >> t3 >> t4 >> t7 >> t8 >> t13 >> t14 >> t9 >> t10

