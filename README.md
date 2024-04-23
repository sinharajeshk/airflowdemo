# airflowdemo
1. Create a WSL/EC2 Linux Machine
2. Linux User - airflow
3. Execute below commands on Linux Machine to create environment for Airflow
		a. sudo apt update
		b. sudo apt install python3-pip
		c. sudo apt install python3.10-venv
		d. python3 -m venv airflowdemo-venv (virtual environment)
		e. source airflowdemo-venv/bin/activate (Activate virtual environment)
		f. sudo pip install pandas
		g. sudo pip install s3fs
		h. pip install Flask-Session==0.5.0
		i. sudo pip install apache-airflow
		j. pip install apache-airflow-providers-imap- (support email sending)
    k. pip install apache-airflow-providers-microsoft-azure (using azure storage account)
		l. pip install apache-airflow-providers-amazon
    m. airflow standalone
4. Open http://localhost:8080
   a. user name : admin
   b. password: password will be shown on console after running airflow standalone command
5. create below folders on the linux machine:
   a. /home/airflow/data
   b. /home/airflow/output
   c. /home/airflow/archive
   d. /home/airflow/report
6. create S3 bucket name : airflowdemo on AWS
7. create Lambda functions on AWS (code available in lambda folder)
8. setup trigger on S3 for one of the lambda function
9. Create a folder dags under /home/airflow/airflow and copy the files from dag folder
10. environment is setup
11. Now drop the file people.csv with name people_<yyyymmdd>.csv in the folder /home/airflow/data
12. trigger the dag process_files 


