from datetime import datetime
from airflow import DAG
# BranchPythonOperator - will be used for task environment_branch to parse the environment type parameter given by 
# the user and choose the correct workflow branch to execute.
# PythonOperator - will be used for task file_creation to create the file according to the user parameter,
# + will be used for task print_to_console to read the newly created file and print its content to the console.
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago


# used as the python_callable for the BranchPythonOperator, It parses the environment_type parameter provided
# by the user and returns the name of the task to execute based on the parameter value.
def choose_branch(**kwargs):
    env_type = kwargs['dag_run'].conf['environment_type']
    if env_type == 'development':
        return 'file_creation_development'
    elif env_type == 'production':
        return 'file_creation_production'
    else:
        raise ValueError(f"Invalid environment_type parameter: {env_type}")

#  used as the python_callable for the PythonOperator, It creates the file with the appropriate filename and content
#  based on the env_type parameter value.
def create_file(env_type, **kwargs):
    if env_type == 'development':
        filename = f'civalue_development_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.txt'
        file_content = 'hello ciValue from development branch'
    elif env_type == 'production':
        filename = f'civalue_production_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.txt'
        file_content = 'hello ciValue from production branch'
    else:
        raise ValueError(f"Invalid environment_type parameter: {env_type}")
    with open(filename, 'w') as f:
        f.write(file_content)
    kwargs['ti'].xcom_push(key='file_path', value=filename)
    print(filename)

# used as the python_callable for the PythonOperator, It reads the file created in the previous task and prints its content to the console.
def print_file_content(**kwargs):
    filename = kwargs['ti'].xcom_pull(key='file_path')
    with open(filename, 'r') as f:
        print(f.read())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    # used to enable the DAG context to be passed to the functions, allowing us to access the dag_run
    'provide_context': True,
}

dag = DAG(
    'file_creation_dag',
    default_args=default_args,
    schedule_interval=None,
)

environment_branch = BranchPythonOperator(
    task_id='environment_branch',
    python_callable=choose_branch,
    dag=dag,
)

file_creation_development = PythonOperator(
    task_id='file_creation_development',
    python_callable=create_file,
    op_kwargs={'env_type': 'development'},
    dag=dag,
)

file_creation_production = PythonOperator(
    task_id='file_creation_production',
    python_callable=create_file,
    op_kwargs={'env_type': 'production'},
    dag=dag,
)

print_to_console = PythonOperator(
    task_id='print_to_console',
    python_callable=print_file_content,
    dag=dag,
    trigger_rule='one_success'
)

environment_branch >> [file_creation_development, file_creation_production] >> print_to_console
