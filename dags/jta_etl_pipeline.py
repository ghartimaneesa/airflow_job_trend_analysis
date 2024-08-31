from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
from scripts.cleaning_raw_data import start_cleaning_data as scd
from scripts.linkedin_scraping import start_linkedin_scraping as ls
from scripts.simply_hired_scraping_script import start_scraping as sc
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

list_to_send_mail = ['ghartimaneesa26@gmail.com', 'singh851singh@gmail.com', 'rauniyarscarlet079@gmail.com']

def success_function(context):
    dag_run = context.get('dag_run')
    task_id = context.get('task_instance').task_id  # Get the current task ID
    msg = """
    <h3>{task_id} dag ran successfully</h2>
    <p>Task ID: {task_id}</p>
    <p>DAG run ID: {dag_run_id}</p>
    <p>DAG run execution date: {execution_date}</p>
    <p>Timestamp: {timestamp}</p>
    """.format(
        task_id=task_id,
        dag_run_id=dag_run.run_id,
        execution_date=context['execution_date'],
        timestamp=datetime.now()
    )
    subject = f"DAG {dag_run.run_id} has completed"
    email_operator = EmailOperator(
        task_id='send_success_email',
        to=['ghartimaneesa26@gmail.com', 'singh851singh@gmail.com'],
        subject=subject,
        html_content=msg
    )
    email_operator.execute(context=None)

def failure_function(context):
    dag_run = context.get('dag_run')
    task_id = context.get('task_instance').task_id  # Get the current task ID
    msg = """
    <h3>{task_id} dag has failed</h2>
    <p>Task ID: {task_id}</p>
    <p>DAG run ID: {dag_run_id}</p>
    <p>DAG run execution date: {execution_date}</p>
    <p>Failure Timestamp: {timestamp}</p>
    """.format(
        task_id=task_id,
        dag_run_id=dag_run.run_id,
        execution_date=context['execution_date'],
        timestamp=datetime.now()
    )
    subject = f"JTA DAG Failed"
    email_operator = EmailOperator(
    to=['ghartimaneesa26@gmail.com', 'singh851singh@gmail.com'],
    subject=subject, 
    html_content=msg)
    email_operator.execute(context=None)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'email': ['[email protected]'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

jta_glue_crawler_config = {
    "Name": 'job-trend-analysis-crawler',
    "DatabaseName": 'job-trend-analysis-db',
    "Targets": {
        "S3Targets": [
            {
                "Path": "s3://job-trend-analysis-data/raw_data/"
            }
        ]
    }
}
with DAG (
    'job_trend_analysis',
    default_args=default_args,
    catchup=False,
    description='A simple DAG to execute a Python script',
    schedule_interval=timedelta(days=1),
) as dag:

    task1 = PythonOperator(
        python_callable= sc,
        task_id = 'trigger_simply_hired_scraping',
        on_failure_callback =failure_function,
        email = list_to_send_mail
    )

    task2 = PythonOperator(
        task_id = 'trigger_linkedin_scraping',
        python_callable = ls,
        on_failure_callback =failure_function,
        email = list_to_send_mail
    )

    task3 = GlueCrawlerOperator(
      task_id = "JTA_Trigger_Crawler",
      config = jta_glue_crawler_config,
      aws_conn_id = "aws_connection",
      on_failure_callback =failure_function,
      email = list_to_send_mail
    ) 

    task4 = PythonOperator(
        task_id = 'clean_raw_data',
        python_callable = scd,
        on_failure_callback =failure_function,
        email = list_to_send_mail
    )

    send_email = EmailOperator(
        task_id = 'send_mail_after_pipeline_completion',
        to=['ghartimaneesa26@gmail.com', 'singh851singh@gmail.com'],
        subject= 'Job Trend Analysis Pipeline Status',
        html_content='''<h3>All DAG of pipeline ran successfully</h3>
                        <br>
                        <p>
                            trigger_simply_hired_scraping: Success
                            <br>
                            trigger_linkedin_scraping: Success
                            <br
                            JTA_Trigger_Crawler: Success
                            <br>
                            clean_raw_data: Success
                        </p>
                        '''

    )

[task1, task2] 
[task1, task2] >> task3 >> task4 >> send_email

# task2 >> task4 