from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('test_dataverk', start_date=days_ago(1), schedule_interval=None) as dag:
    t1 = python_operator(dag=dag,
                         name="hello_world",
                         repo="navikt/vdl-eiendom",
                         branch="teste-dataverk-operators",
                         script_path="test_airflow.py")
    
    t1