from airflow.decorators import dag, task
import pendulum

@task
def simple_fda_task():
    print("FDA Simple Test - DAG está funcionando!")
    return "success"

@dag(
    dag_id='fda_simple_test',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule='@weekly',
    catchup=False,
    tags=['fda', 'test'],
)
def fda_simple_dag():
    simple_fda_task()

dag = fda_simple_dag()
