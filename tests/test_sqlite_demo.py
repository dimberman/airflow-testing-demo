import os
from datetime import datetime
from dags.sqlite_demo import top_five_animations
from airflow.models.dag import DAG
from astro import sql as aql
from astro.files import File
from astro.table import Table
import pandas as pd

conn_path = os.getenv("CONF_FILE_PATH")


def test_top_five_animations(check_dag_output):
    @aql.dataframe()
    def validate(df_to_validate: pd.DataFrame):
        assert len(df_to_validate) == 5

    dag = DAG(dag_id="test_top_five_animations", start_date=datetime.now())
    with dag:
        imdb_movies = aql.load_file(
            input_file=File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
            output_table=Table(conn_id="special_sqlite_conn"),
        )
        top_five_movies_task = top_five_animations(imdb_movies)
        validate(top_five_movies_task)
        aql.cleanup()
    dag.test(conn_file_path=conn_path)
