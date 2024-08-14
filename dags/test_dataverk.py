from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('test_dataverk', start_date=days_ago(1), schedule_interval=None) as dag:
    t1 = python_operator(dag=dag,
                         name="hello_world",
                         repo="navikt/vdl-eiendom",
                         branch="teste-dataverk-operators",
                         script_path="test_airflow.py",
                         image='europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-inbound@sha256:df16552fbba6affd60fc3c47ca3a1705f4ca5fa39bd396fe5393395a2dcd17e1')
    
    t1