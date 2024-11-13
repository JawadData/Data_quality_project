from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  
import papermill as pm

def run_notebook_completude():
    pm.execute_notebook(
        '/opt/airflow/data/completude_tests.ipynb',  
        '/opt/airflow/data/completude_tests_results.ipynb',
        kernel_name='python3' 
    )

def run_notebook_validite():
    pm.execute_notebook(
        '/opt/airflow/data/validite_tests.ipynb',
        '/opt/airflow/data/validite_tests_results.ipynb',
        kernel_name='python3' 
    )

def run_notebook_coherence():
    pm.execute_notebook(
        '/opt/airflow/data/coherence_tests.ipynb', 
        '/opt/airflow/data/coherence_tests_results.ipynb',
        kernel_name='python3'  
    )

def run_notebook_unicite():
    pm.execute_notebook(
        '/opt/airflow/data/unicite_tests.ipynb', 
        '/opt/airflow/data/unicite_tests_results.ipynb',
        kernel_name='python3'  
    )


def run_notebook_trans_final_data():
    pm.execute_notebook(
        '/opt/airflow/data/transform_final.ipynb',
        '/opt/airflow/data/transform_final_results.ipynb',
        kernel_name='python3'  
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 30),
    'retries': 1,
}

dag = DAG(
    'notebook_execution_dag',
    default_args=default_args,
    description='DAG pour exécuter un notebook Jupyter',
    schedule_interval=None,  
)

run_notebook_completude_task = PythonOperator(
    task_id='run_notebook_completude_task',
    python_callable=run_notebook_completude,
    dag=dag  
)

run_notebook_validite_task = PythonOperator(
    task_id='run_notebook_validite_task',
    python_callable=run_notebook_validite,
    dag=dag  
)
run_notebook_coherence_task = PythonOperator(
    task_id='run_notebook_coherence_task',
    python_callable=run_notebook_coherence,
    dag=dag
)
run_notebook_unicite_task = PythonOperator(
    task_id='run_notebook_unicite_task',
    python_callable=run_notebook_unicite,
    dag=dag  
)

run_notebook_task_finale = PythonOperator(
    task_id='run_notebook_task_final',
    python_callable=run_notebook_trans_final_data,
    dag=dag  
)

run_notebook_completude_task >> run_notebook_validite_task >> run_notebook_coherence_task >> run_notebook_unicite_task >> run_notebook_task_finale
