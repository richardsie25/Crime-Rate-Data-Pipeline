from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# File paths
AIRBNB_SCRIPT_PATH = "/home/final_project/team18/airbnb.py"
CRIME_SCRIPT_PATH = "/home/final_project/team18/crime.py"
AIRBNB_OUTPUT_PATH = "/home/final_project/la_dataset/cleaned_listings"
CRIME_OUTPUT_PATH = "/home/final_project/la_dataset/cleaned_LA_crimedata"
DUCKDB_EXECUTABLE = "duckdb"
DUCKDB_DATABASE = "/home/final_project/team18/final_project.db"
DUCKDB_QUERIES = "/home/final_project/team18/DuckDB_query.sql"

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 23),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'airbnb_crime_to_duckdb',
    default_args=default_args,
    timetable=None,
    catchup=False
)

# Task 1: Run Spark job to process Airbnb data
run_airbnb_task = BashOperator(
    task_id='run_airbnb_job',
    bash_command=f'''
        spark-submit --master local[*] \
            --driver-memory 2g \
            --executor-memory 2g \
            --conf spark.sql.shuffle.partitions=4 \
            {AIRBNB_SCRIPT_PATH}
    ''',
    cwd="/home/final_project/team18",
    dag=dag,
)

# Task 2: Run Spark job to process Crime data
run_crime_task = BashOperator(
    task_id='run_crime_job',
    bash_command=f'''
        spark-submit --master local[*] \
            --driver-memory 2g \
            --executor-memory 2g \
            --conf spark.sql.shuffle.partitions=4 \
            {CRIME_SCRIPT_PATH}
    ''',
    cwd="/home/final_project/team18",
    dag=dag,
)

# Task 3: Load Parquet and CSV into DuckDB
load_duckdb_task = BashOperator(
    task_id='load_parquet_into_duckdb',
    bash_command=f'''
        LOAD_AIRBNB_PATH="{AIRBNB_OUTPUT_PATH}/*.parquet" &&
        LOAD_CRIME_PATH="{CRIME_OUTPUT_PATH}/*.csv" &&
        sed "s|\$LOAD_AIRBNB_PATH|${{LOAD_AIRBNB_PATH//\//\/}}|g" "{DUCKDB_QUERIES}" | {DUCKDB_EXECUTABLE} "{DUCKDB_DATABASE}" &&
        sed "s|\$LOAD_CRIME_PATH|${{LOAD_CRIME_PATH//\//\/}}|g" "{DUCKDB_QUERIES}" | {DUCKDB_EXECUTABLE} "{DUCKDB_DATABASE}"
    ''',
    dag=dag
)

# Set task dependencies
run_airbnb_task >> run_crime_task >> load_duckdb_task
