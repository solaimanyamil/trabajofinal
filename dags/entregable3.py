import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator
from funciones import load



#               *************          VARIABLES              *************  

db_username = Variable.get('db_username')
db_password = Variable.get('db_password')
db_name = Variable.get('db_name')
db_host = Variable.get('db_host')
db_port = Variable.get('db_port')

#               ************* PARÁMETROS Y DEFINICIÓN DEL DAG *************

default_args={
    'owner': 'Yamil',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)}

BC_dag = DAG(
    default_args=default_args,
    dag_id='entregable3',
    description='DAG para extracción, transformación y carga de datos',
    start_date=datetime(2023,9,2),
    schedule_interval="@daily",
    catchup=False)

#               ************* DEFINICIÓN DE TAREAS *************

# 1. Extracción, Transformación y Carga:
load = PythonOperator(
    task_id='extract_and_transform_data',
    python_callable=load,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag)

