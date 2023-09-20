import os

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from astro import sql as aql
from astro.files import File
from astro.table import Table
from datetime import datetime

@aql.transform()
def top_five_animations(input_table: Table):
    return """
        SELECT title, rating
        FROM {{input_table}}
        WHERE genre1='Animation'
        ORDER BY Rating desc
        LIMIT 5;
    """


@aql.dataframe(columns_names_capitalization="original")
def top_five_animations_df(input_df: pd.DataFrame):
    print(f"Total Number of records: {len(input_df)}")
    top_5_movies = input_df.sort_values(by="rating", ascending=False)[["title", "rating", "genre1"]].head(5)
    print(f"Top 5 Movies: {top_5_movies}")
    return top_5_movies


with DAG(  # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
        # will run daily
        schedule="@daily",
        # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
        # start_date. Subsequent DAG runs are instantiated based on the schedule
        start_date=datetime(2023, 1, 1),
        # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
        # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
        # run will be for the next 30 mins, per the its schedule
        catchup=False,
        default_args={
            "retries": 2,  # If a task fails, it will retry 2 times.
        },
        dag_id="sqlite_dag",
        tags=["example"], ) as dag:
    imdb_movies = aql.load_file(
        input_file=File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        output_table=Table(conn_id="special_sqlite_conn"),
    )
    top_five_movies_task = top_five_animations(input_table=imdb_movies)
    top_five_movies_task_df = top_five_animations_df(input_df=imdb_movies)


    # The calculation of top 2 movies is purposely done in a separate task using @task decorator
    # so that we can test that dataframe is correctly stored in XCom and passed to the following task
    @task
    def top_two_movies(input_df: pd.DataFrame):
        top_2 = input_df.head(2)
        print(f"Top 2 movies: {top_2}")


    top_two_movies(top_five_movies_task_df)
    # aql.cleanup()

if __name__ == "__main__":
    conn_path = os.getenv("CONN_FILE_PATH")
    dag.test(conn_file_path=conn_path)
