import os
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG
from dotenv import load_dotenv

load_dotenv()

with DAG(
    "collect_store_schedule_data",
    description="""
    Download the gtfs schedule data of the Reseau Azur Transport at Nice,
    clean it and store it in the duckDB database.
    """,
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2025, 9, 5),
    end_date=pendulum.datetime(2025, 9, 30),
    default_args={"retries": 1},
    tags=["reseauazur"],
    max_active_runs=1,
):
    doc_md = """
    # Test
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t0 = EmptyOperator(task_id="starting")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t1 = BashOperator(
        task_id="collect_schedule_data_file",
        bash_command=f"""
        cd {os.environ["WORK_DIR"]}/data;
        curl -o gtfs-schedule.zip {os.environ["GTFS_SCHEDULE_URL"]}
        """,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t2 = BashOperator(
        task_id="unzip_schedule_data_file",
        bash_command=f"""
        cd {os.environ["WORK_DIR"]}/data;
        unzip -d schedule_temp -o gtfs-schedule.zip
        """,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    s3_1 = BashSensor(
        task_id="check_routes_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh routes.txt",
        retry_exit_code=1,
        poke_interval=10,
        timeout=60,
    )

    s3_2 = BashSensor(
        task_id="check_stops_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh stops.txt",
        retry_exit_code=1,
        poke_interval=10,
        timeout=60,
    )

    s3_3 = BashSensor(
        task_id="check_trips_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh trips.txt",
        retry_exit_code=1,
        poke_interval=10,
        timeout=60,
    )

    s3_4 = BashSensor(
        task_id="check_stop_times_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh stop_times.txt",
        retry_exit_code=1,
        poke_interval=10,
        timeout=60,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t4_1 = BashOperator(
        task_id="copy_routes_file",
        bash_command=f"""
        cp {os.environ["WORK_DIR"]}/data/schedule_temp/routes.txt {os.environ["WORK_DIR"]}/data/schedule/
        """,
    )

    t0 >> t1 >> t2
    s3_1 >> t4_1
