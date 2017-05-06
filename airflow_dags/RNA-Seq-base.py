"""
A test to see if the RNA-seq workflow can be run using airflow.
This file will be a "base" for workflows using airflow?
"""

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

import imp

default_args = {
    'es_index_host' : 'localhost',
    'es_index_port' : '9200',
    'redwood_token' : 'must_be_defined',
    'redwood_host' : 'storage.ucsc-cgl.org',
    'image_descriptor' : 'must_be_defined',
    'dockstore_tool_running_dockstore_tool' : 'quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8',
    'tmp_dir' : '/datastore',
    'max_jobs' : 1,
    'touch_file_bucket' : 'must_be_defined',
    'workflow_name' : 'RNA_Seq_airflow',
    'workflow_path' : './airflow_executors',
    'start_date': airflow.utils.dates.days_ago(2),
    'test_mode' : False,
}

base_dag = DAG(
    dag_id='rnaseq',
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

#save jobs
jobs = []

def populate_jobs():
    #for each iteration of the decider clear jobs (so there aren't any left)
    print "attempt to populate jobs beggining now"
    jobs = []

    args = {}
    args['default_args'] = default_args
    args['unique_args'] = {'test' : 'this_is_a_test', 'anothertest' : 'still_a_test'}

    for i in range(0, 10):
        jobs.append(args) #simply to populate a jobs list

    return "Found "+str(len(jobs))+" jobs to be run through "+default_args['workflow_name']

def execute_workflow():
    print "beggining execution of workflow"
    print (jobs)

    return str(min(len(jobs), int(default_args['max_jobs']))) + " jobs executed"


#TODO : figure out structure for workflows
""" scan metadata, if jobs are returned run them with dockstore or consonance """
scan_metadata = PythonOperator(
    task_id='scan_metadata',
    python_callable=populate_jobs,
    dag=base_dag)

""" run specified workflow via tool of choice (dockstore, consonance, ???) """
run_execution_tool = PythonOperator(
    task_id='run_execution_tool',
    python_callable=execute_workflow,
    dag=base_dag)

#so that metadata is scanned before consonance/dockstore are run
scan_metadata.set_downstream(run_execution_tool)
