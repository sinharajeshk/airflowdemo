from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
import logging
import os
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# Set up logging
logger = logging.getLogger(__name__)

def download_file_from_s3(*args, **kwargs):    
    hook = S3Hook(aws_conn_id='aws_connection')
    s3_bucket_name='airflowdemo'
    current_date = datetime.now().strftime('%Y%m%d')
    file_key='output/result_'+current_date +'.html'
    archive_file_key='airflowdemo/archive/result_'+current_date +'.html'
    
    local_path='/home/airflow/report'
    dest_file_name = local_path +'/result_'+current_date +'.html'
    if hook.get_conn():
        print('Connection successful!')
    else:
        print('Connection failed!')
    # Check if the key exists
    if hook.check_for_key(file_key, s3_bucket_name):
        logger.info(f'File found at S3 file key {file_key}, bucketname {s3_bucket_name}.')
        # If the key exists, download the file 
        try:
            file_name = hook.download_file(key= file_key, bucket_name=s3_bucket_name , local_path=local_path)
            print(f"File {file_name} downloaded Successfully.")            
            os.rename(file_name, dest_file_name)
            logger.info(f'File downloaded to S3 Successfully.')
            
            hook.delete_objects(bucket=s3_bucket_name, keys=file_key)
            logger.info(f'File deleted from output folder Successfully.')
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
    else:
        logger.info('File does not exist in S3 bucket')
        return False

def send_email_with_attachment():
    # aws Region name, S3 bucket access key and access key Id to be updated
    ses = boto3.client('ses', 
                       region_name='', 
                       aws_access_key_id='', 
                       aws_secret_access_key='')
    
    current_date = datetime.now().strftime('%Y%m%d')
    attachment_file_name='/home/airflow/report/result_'+current_date +'.html'

    subject = 'Report - ' + current_date
    body_text = 'PFA reports for ' + current_date
    sender = 'sender@example.com'
    recipient = 'receiver@example.com'
    attachment = attachment_file_name

    try:
        with open(attachment, 'rb') as f:
            data = f.read()

        response = ses.send_raw_email(
            Source=sender,
            Destinations=[
                recipient
            ],
            RawMessage={
                'Data': f'''From: {sender}
To: {recipient}
Subject: {subject}
MIME-Version: 1.0
Content-type: Multipart/Mixed; boundary="NextPart"

--NextPart
Content-Type: text/plain

{body_text}

--NextPart
Content-Type: text/plain; 
Content-Disposition: attachment; filename="{os.path.basename(attachment)}"

{data}

--NextPart--'''
            }
        )

        print(f'Email sent! Message Id: {response["MessageId"]}')
    except FileNotFoundError:
        print('Attachment not found.')
    except NoCredentialsError:
        print('No AWS credentials found.')
    except Exception as e:
        print(f'Error: {e}')


dag = DAG(
    dag_id='s3_download_send_report',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
)   
  
download_file_task = PythonOperator(
    task_id='download_report',
    python_callable=download_file_from_s3,
    dag=dag
)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email_with_attachment,
    dag=dag)


s3_file_sensor = S3KeySensor(
    task_id='s3_file_sensor',
    bucket_key="s3://airflowdemo/output/result_"+datetime.now().strftime('%Y%m%d')+'.html',
    wildcard_match=True,
    aws_conn_id='aws_connection',
    timeout=18*60*60,
    poke_interval=60,
    dag=dag)

# Start with the initial task
start_task = DummyOperator(task_id='start_task', dag=dag)
# End task
end_task = DummyOperator(task_id='end_task', dag=dag)
start_task >> s3_file_sensor >> download_file_task >> send_email_task>> end_task

