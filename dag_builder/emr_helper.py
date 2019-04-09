#!/usr/bin/env python3

"""
DAG Factory methods to create an EMR Cluster, run steps and terminate cluster
Developer: Gokturk (GT) Yurtyapan
e-mail: gt.yurtyapan@starz.com
Date: 2019-04-09
"""

from datetime import datetime, timedelta
import airflow
import json
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator \
    import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator \
    import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator


def run_emr_job(current_dag, cluster_name, task_gen_name, aws_connection, emr_connection,
                script_location, library_location, region='us-east-1'):
    """
    Creates the EMR cluster, runs the step and terminates the cluster when it is completed
    current_dag: DAG that is created by the user
    cluster_name: Name given to cluster by the user
    task_gen_name: A general name for the task being done. This is used to name different tasks
    aws_connection: Connection to AWS for account credentials
    emr_connection: Name of Airflow connection storing EMR configuration details
    script_location: S3 location of the xcript to be run
    library_location: S3 location of the library being used to run spark-submit
    region: AWS region where the cluster is being created
    """

    # Name of the new cluster being created
    job_flow_overrides = {
        'Name': cluster_name
    }

    # name of task creating the cluster
    create_cluster_task_name = task_gen_name + "_create_cluster"

    # Task that creates the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id= create_cluster_task_name,
        job_flow_overrides=job_flow_overrides,
        aws_conn_id=aws_connection,
        emr_conn_id=emr_connection,
        dag=current_dag
    )

    # script-runner.jar file location is region specific
    script_runner_jar = 's3://' + region + '.elasticmapreduce/libs/script-runner/script-runner.jar'

    # Step description
    step_definition = [
        {
            'Name': task_gen_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': script_runner_jar,
                'Args': [script_location, library_location, '']
            }
        }
    ]

    # Task that terminates the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id=task_gen_name + "_remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull('" + create_cluster_task_name + "', key='return_value') }}",
        aws_conn_id=aws_connection,
        dag=current_dag
    )

    # Add the step and step checker tasks
    add_step_to_emr(cluster_creator, task_gen_name, step_definition, cluster_remover,
                    create_cluster_task_name, aws_connection, current_dag)


def add_step_to_emr(cluster_create_task, task_identifier, step_params, cluster_remover, task_create_cluster,
                    aws_connection, dag):
    """
    In case we need to add multiple steps to the cluster
    cluster_create_task: ID of task that creates a cluster
    task_identifier: ID of step
    step_params: parameters to pass to the step
    cluster_remover: task that terminates the cluster
    task_create_cluster: task that creates the cluster
    aws_connection: Connection to AWS for account credentials
    dag: DAG that is created by the user
    """
    step_adder = EmrAddStepsOperator(
        task_id=task_identifier,
        job_flow_id="{{ task_instance.xcom_pull('" + task_create_cluster + "', key='return_value') }}",
        aws_conn_id=aws_connection,
        steps=step_params,
        dag=dag
    )

    step_checker = EmrStepSensor(
        task_id=task_identifier + '_watch_step',
        job_flow_id="{{ task_instance.xcom_pull('" + task_create_cluster + "', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('" + task_identifier + "', key='return_value')[0] }}",
        aws_conn_id=aws_connection,
        dag=dag
    )

    cluster_create_task.set_downstream(step_adder)
    step_adder.set_downstream(step_checker)
    step_checker.set_downstream(cluster_remover)
