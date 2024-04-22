import csv
import sqlite3
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.S3_hook import S3Hook
import re

import pandas as pd
import shutil
import logging
#from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Set up logging
logger = logging.getLogger(__name__)

#def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    file_path =  ti.xcom_pull(task_ids='get_file_path', key='output_file_path') 
    file_key, file_extension = os.path.splitext(os.path.basename(file_path))
    file_key = f"{file_key}{file_extension}"
    bucket_name='rs-airflowdemo'    
    logger.info(f'Source File Path {file_path}, file key {file_key}, bucket Name {bucket_name}')
        
    hook = S3Hook('aws_connection')
    if hook.get_conn():
        print('Connection successful!')
    else:
        print('Connection failed!')
    # Check if the key exists
    if hook.check_for_key(file_key, bucket_name):
        # If the key exists, delete the file before uploading
        hook.delete_objects(bucket=bucket_name, keys=file_key)
    
    hook.load_file(filename=file_path, key=file_key, bucket_name=bucket_name)
    logger.info(f'File loaded to S3 Successfully.')


def is_valid_email(email):
    email_regex = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
    return email_regex.match(email) is not None

def process_and_validate_create_cleaned_data(**kwargs):
   
    ti = kwargs['ti']
    output_csv_file_path =  ti.xcom_pull(task_ids='get_file_path', key='output_file_path') 
    src_file_path =  ti.xcom_pull(task_ids='get_file_path', key='src_file_path') 
    logger.info(f'Source File Path {src_file_path}.')
    logger.info(f'Output File Path {output_csv_file_path}.')
    
     # Read the CSV file
    
    data = pd.read_csv(src_file_path)

    # Define a list to store the valid rows
    valid_rows = []

    for index, row in data.iterrows():
        # Check if the FirstName is not null
        if pd.notna(row['FirstName']):
            # Check if the Email is valid
            if is_valid_email(row['Email']):
                # Concatenate FirstName and LastName
                row['Name'] = row['FirstName'] + ' ' + row['LastName']

                # Append the valid row to the list
                valid_rows.append(row)

    # Create a DataFrame from the list of valid rows
    valid_data = pd.DataFrame(valid_rows)
    valid_data.dropna(subset=['Index'], inplace=True)
    # Write the valid rows to a new CSV file
    valid_data.to_csv(output_csv_file_path, index=False)
        
# Removing blank line at the end of CSV
    with open(output_csv_file_path, 'r') as inp, open('temp.csv', 'w', newline='') as out:
        writer = csv.writer(out)
        for row in csv.reader(inp):
            if row:
                writer.writerow(row)

        os.remove(output_csv_file_path)
        os.rename('temp.csv', output_csv_file_path)
    logger.info(f'Clean Data File created after validation and transformation at {output_csv_file_path}.')
    

#def read_and_display_sqlite_data(**kwargs):
#    db_file_path = 'people.db'
#    table_name ='people'
#    conn = sqlite3.connect(db_file_path)
#    cursor = conn.cursor()
#    current_date = datetime.now()
#    cursor.execute(f"SELECT * FROM {table_name} where DATE(Date_Created) = DATE('{current_date}')")
#    rows = cursor.fetchall()
#    
#    for row in rows:
#        print(row)    
#    conn.close()

def get_file_path(**kwargs):
    file_name = 'people'
    file_ext = '.csv'
    #Source file path
    src_dir = '/home/airflow/data/'
    current_date = datetime.now().strftime('%Y%m%d')
    src_file_name = f"{file_name}_{current_date}{file_ext}"
    src_file_path = os.path.join(src_dir, src_file_name)
    kwargs['ti'].xcom_push(key='src_file_path', value=src_file_path)
    logger.info(f'Source File Path {src_file_path}.')
    # Archive file path
    archive_dir = '/home/airflow/archive/'
    file_name, file_extension = os.path.splitext(os.path.basename(src_file_path))
    current_datetime = datetime.now().strftime('%H%M%S')
    archive_file_name = f"{file_name}_{current_datetime}{file_extension}"
    archive_file_path = os.path.join(archive_dir, archive_file_name)
    kwargs['ti'].xcom_push(key='archive_file_path', value=archive_file_path)
    logger.info(f'Archive File Path {archive_file_path}.')

    # Output file path
    output_dir = '/home/airflow/output/'
    file_name, file_extension = os.path.splitext(os.path.basename(src_file_path))
    current_datetime = datetime.now().strftime('%H%M%S')
    output_file_name = f"{file_name}_{current_datetime}_Cleaned{file_extension}"
    output_file_path = os.path.join(output_dir, output_file_name)
    kwargs['ti'].xcom_push(key='output_file_path', value=output_file_path)
    logger.info(f'output File Path {output_file_path}.')

    
def archive_file(**kwargs):
    ti = kwargs['ti']
    src_file_path = ti.xcom_pull(task_ids='get_file_path', key='src_file_path')
    archive_file_path = ti.xcom_pull(task_ids='get_file_path', key='archive_file_path') 
    logger.info(f'Source File Path {src_file_path}, Archive File Path {archive_file_path}.')

    if os.path.exists(src_file_path):
        shutil.copy(src_file_path, archive_file_path)
        logger.info(f"File copied from {src_file_path} to {archive_file_path}")
        

def remove_file(**kwargs):
    ti = kwargs['ti']
    src_file_path = ti.xcom_pull(task_ids='get_file_path', key='src_file_path')

    logger.info(f'Source File Path {src_file_path}.')
    file_name, file_extension = os.path.splitext(os.path.basename(src_file_path))
    
    if os.path.exists(src_file_path):
        os.remove(src_file_path)
        logger.info(f'File {src_file_path} has been removed.')
        print(f'File {src_file_path} has been removed.')
    else:
        logger.info(f'File {src_file_path} does not exist.')
        print(f'File {src_file_path} does not exist.')

#def import_csv_to_sqlite(**kwargs):
#    db_file ='people.db'
#    ti = kwargs['ti']
#    src_file_path = ti.xcom_pull(task_ids='get_file_path', key='src_file_path')
#    src_file_name = os.path.basename(src_file_path).split('/')[-1]
#    logger.info(f'Source File Path: {src_file_path} ,Name: {src_file_name}.')
#
#    conn = sqlite3.connect(db_file)
#    cur = conn.cursor()
#
#    cur.execute('''
#        CREATE TABLE IF NOT EXISTS people (
#            "Index" INTEGER,
#            UserId TEXT,
#            FirstName TEXT,
#           LastName TEXT,
#            Sex TEXT,
#            Email TEXT,
#            Phone TEXT,
#            Dateofbirth TEXT,
#            JobTitle TEXT,
#            Src_File_Name TEXT,
#            Date_Created  TEXT                            
#        )
#    ''')
#    conn.commit()
#
#    with open(src_file_path, 'r') as file:
#        reader = csv.reader(file)
#        next(reader)
#        for row in reader:
#            cur.execute('''
#                INSERT INTO people ("Index", UserId, FirstName,LastName,Sex,Email,Phone,Dateofbirth,JobTitle,Src_File_Name,Date_Created)
#                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#            ''', row+[src_file_name,datetime.now()])
#        conn.commit()
#
#    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 1, 1),
}
    
dag = DAG(
    dag_id='process_files',
    default_args=default_args,
    description='Process CSV to SQLite using Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

file_sensor_task = FileSensor(
    task_id='file_sensor',
    #src_dir = '/home/airflow/data/',  # Replace with the path to the file you want to check
    filepath=os.path.join('/home/airflow/data/', 'people_' + datetime.now().strftime('%Y%m%d')+'.csv'),
    #filepath="{{ task_instance.xcom_pull(task_ids='get_file_path_task', key='src_file_path') }}",
    fs_conn_id='fs_default',  # This is the default connection ID for the local filesystem
    timeout=300,  # The total time in seconds to wait for the file to appear, default is 7 days
    poke_interval=60,  # The time in seconds between checks for the file's existence, default is 5 minutes
    mode='poke',  # The mode in which the sensor operates, default is 'poke'
    dag=dag,
)

#csv_to_sqlite_task = PythonOperator(
#    task_id='import_csv_to_sqlite',
#    provide_context=True,
#    python_callable=import_csv_to_sqlite,
#    #op_args=['/home/airflow/data/people_20240312.csv', 'example.db'],
#    dag=dag,
#)

get_file_path_task = PythonOperator(
    task_id='get_file_path',
    python_callable=get_file_path,
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

#read_sqlite_data_task = PythonOperator(
#    task_id='read_sqlite_data',
#    python_callable=read_and_display_sqlite_data,
#    provide_context=True,
#    dag=dag
#)

# Call the function with the path to your CSV file and output CSV file path
validate_transform_and_create_file_task = PythonOperator(
    task_id='process_and_validate_create_cleaned_data',
    python_callable=process_and_validate_create_cleaned_data,
    provide_context=True,
    #op_args=['/path/to/your/csv_file.csv', '/path/to/your/output_csv_file.csv'],
    dag=dag
)


archive_file_task = PythonOperator(
    task_id='archive_file',
    python_callable=archive_file,
    provide_context=True,
    dag=dag,
)

delete_file_task = PythonOperator(
    task_id='remove_file',
    python_callable=remove_file,
    provide_context=True,
    dag=dag,
)
# Upload the file
upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag
        
)


# Start with the initial task
start_task = DummyOperator(task_id='start_task', dag=dag)
# End task
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> file_sensor_task >> end_task
start_task>> file_sensor_task>> get_file_path_task >> archive_file_task >> validate_transform_and_create_file_task >> upload_to_s3_task >> delete_file_task >> end_task

