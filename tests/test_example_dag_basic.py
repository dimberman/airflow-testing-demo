from dags.example_dag_basic_easy_to_test import load, transform, extract
from airflow.models.dag import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.operators.bash import BashOperator


def test_load(capsys):
    dag = DAG(dag_id="test_top_five_animations", start_date=datetime.now())
    with dag:
        load(5)
    dag.test()
    assert "Total order value is: 5.00" in capsys.readouterr().out


def test_transform(check_dag_output):
    @task
    def validate(order_dict: dict):
        assert order_dict["total_order_value"] == 15

    dag = DAG(dag_id="test_top_five_animations", start_date=datetime.now())
    with dag:
        t = transform({"foo": 5, "bar": 10})
        validate(t)
    dag.test()


def test_bash_command(check_dag_output):
    @task
    def validate():
        import os
        assert os.path.exists("/tmp/foo.txt")

    dag = DAG(
        dag_id="test_top_five_animations",
        start_date=datetime.now()
    )
    with dag:
        b = BashOperator(
            task_id="bash_task",
            bash_command="touch /tmp/foo.txt"
        )
        b >> validate()
    dag.test()
