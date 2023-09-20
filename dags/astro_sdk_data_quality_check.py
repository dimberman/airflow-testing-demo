import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from astro import sql as aql
from astro.files import File
from astro.table import Table


def analyze_assertion_results(assertion_results: pd.DataFrame, threshold: float = 0.0):
    fail_proportion = assertion_results.sum() / len(assertion_results)
    assert fail_proportion <= threshold, f"More than {threshold * 100}% of {len(assertion_results)} rows failed the condition: {fail_proportion * 100:.2f}%"


@aql.dataframe()
def check_table(df: pd.DataFrame):
    fail_condition: pd.DataFrame = df['sell'] < df['list']
    analyze_assertion_results(fail_condition, threshold=0.3)


with DAG(
        "data_validation_custom_check",
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
) as dag:
    # [START data_validation__check_table]
    homes_data = aql.load_file(
        File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/python-sdk/tests/data/homes.csv"),
        output_table=Table(conn_id="pg_demo"),
    )
    check_table(homes_data) >> aql.export_to_file(homes_data, File("/tmp/homes.csv"), if_exists="replace")

    # aql.cleanup()

if __name__ == "__main__":
    dag.test(conn_file_path=os.getenv("CONN_FILE_PATH"))