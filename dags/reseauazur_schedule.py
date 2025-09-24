import os
import pendulum
import pandas as pd
from datetime import timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG
from airflow.decorators import task
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from dotenv import load_dotenv

load_dotenv()

with DAG(
    "collect_store_schedule_data",
    description="""
    Download the gtfs schedule data of the Reseau Azur Transport at Nice,
    clean it and store it in the duckDB database.
    """,
    schedule="0 7 * * *",
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

    # Download- - - - - - - - - - - - - - - - - - - - - - - -
    t1 = BashOperator(
        task_id="collect_schedule_data_file",
        bash_command=f"""
        cd {os.environ["WORK_DIR"]}/data;
        curl -o gtfs-schedule.zip {os.environ["GTFS_SCHEDULE_URL"]}
        """,
    )

    # Unzip - - - - - - - - - - - - - - - - - - - - - - - - -
    t2 = BashOperator(
        task_id="unzip_schedule_data_file",
        bash_command=f"""
        cd {os.environ["WORK_DIR"]}/data;
        unzip -d schedule_temp -o gtfs-schedule.zip;
        rm gtfs-schedule.zip
        """,
    )

    # Wait for files- - - - - - - - - - - - - - - - - - - - -
    s3_1 = FileSensor(
        task_id="wait_routes_file",
        filepath=f"{os.environ["WORK_DIR"]}/data/schedule_temp/routes.txt",
        fs_conn_id="airflow_pg_conn",
        poke_interval=2,
    )

    s3_2 = FileSensor(
        task_id="wait_stops_file",
        filepath=f"{os.environ["WORK_DIR"]}/data/schedule_temp/stops.txt",
        fs_conn_id="airflow_pg_conn",
        poke_interval=2,
    )

    s3_3 = FileSensor(
        task_id="wait_trips_file",
        filepath=f"{os.environ["WORK_DIR"]}/data/schedule_temp/trips.txt",
        fs_conn_id="airflow_pg_conn",
        poke_interval=2,
    )

    s3_4 = FileSensor(
        task_id="wait_stop_times_file",
        filepath=f"{os.environ["WORK_DIR"]}/data/schedule_temp/stop_times.txt",
        fs_conn_id="airflow_pg_conn",
        poke_interval=2,
    )

    # Check files - - - - - - - - - - - - - - - - - - - - - -
    t4_1 = BashOperator(
        task_id="check_routes_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh routes",
        skip_on_exit_code=99,
    )

    t4_2 = BashOperator(
        task_id="check_stops_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh stops",
        skip_on_exit_code=99,
    )

    t4_3 = BashOperator(
        task_id="check_trips_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh trips",
        skip_on_exit_code=99,
    )

    t4_4 = BashOperator(
        task_id="check_stop_times_file",
        bash_command=f"{os.environ["WORK_DIR"]}/dags/scripts/check_temp_file_and_db_access.sh stop_times",
        skip_on_exit_code=99,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t5_1 = BashOperator(
        task_id="copy_routes_file",
        bash_command=f"""
        cp {os.environ["WORK_DIR"]}/data/schedule_temp/routes.txt {os.environ["WORK_DIR"]}/data/schedule/routes.csv
        """,
    )

    t5_2 = BashOperator(
        task_id="copy_stops_file",
        bash_command=f"""
        cp {os.environ["WORK_DIR"]}/data/schedule_temp/stops.txt {os.environ["WORK_DIR"]}/data/schedule/stops.csv
        """,
    )

    t5_3 = BashOperator(
        task_id="copy_trips_file",
        bash_command=f"""
        cp {os.environ["WORK_DIR"]}/data/schedule_temp/trips.txt {os.environ["WORK_DIR"]}/data/schedule/trips.csv
        """,
    )

    t5_4 = BashOperator(
        task_id="copy_stop_times_file",
        bash_command=f"""
        cp {os.environ["WORK_DIR"]}/data/schedule_temp/stop_times.txt {os.environ["WORK_DIR"]}/data/schedule/stop_times.csv
        """,
    )

    # Create tables - - - - - - - - - - - - - - - - - - - - -
    @task(
        task_id="create_dim_route_table",
        retries=10,
        retry_delay=timedelta(seconds=10),
    )
    def t6_1():
        """
        (Re)create the dim_route table in DuckDB from the associated csv-file.
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # execute a simple query
        sql_query = f"""
            DROP TABLE IF EXISTS dim_route;
            CREATE TABLE dim_route AS
            SELECT
                route_id AS id,
                route_type AS type,
                route_short_name AS short_name,
                route_long_name AS long_name,
                route_color AS color
            FROM '{os.environ["WORK_DIR"]}/data/schedule/routes.csv';
            ALTER TABLE dim_route ADD PRIMARY KEY (id)
            """
        conn.execute(sql_query)

        sql_query = """
            INSERT INTO dim_route (
                id,
                type,
                short_name,
                long_name,
                color
            ) VALUES (
                'Unknown',
                -1,
                'Unknown',
                'Unknown',
                '000000'
            );
            """
        conn.execute(sql_query)

        print(conn.sql("SELECT COUNT(*) FROM dim_route").fetchone()[0])
        conn.close()

    @task(
        task_id="create_dim_stop_table",
        retries=10,
        retry_delay=timedelta(seconds=10),
    )
    def t6_2():
        """
        (Re)create the dim_stop table in DuckDB from the associated csv-file.
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # execute a simple query
        sql_query = f"""
            DROP TABLE IF EXISTS dim_stop;
            CREATE TABLE dim_stop AS
            SELECT
                stop_id AS id,
                parent_station,
                stop_name AS name,
                stop_lat AS lat,
                stop_lon AS lon,
                location_type
            FROM '{os.environ["WORK_DIR"]}/data/schedule/stops.csv'
            WHERE location_type IN (0,1);
            ALTER TABLE dim_stop ADD PRIMARY KEY (id);
            """
        conn.execute(sql_query)
        print(conn.sql("SELECT COUNT(*) FROM dim_stop").fetchone()[0])
        conn.close()

    @task(
        task_id="create_dim_trip_table",
        retries=10,
        retry_delay=timedelta(seconds=10),
    )
    def t6_3():
        """
        (Re)create the dim_trip table in DuckDB from the associated csv-file.
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # execute a simple query
        sql_query = f"""
            DROP TABLE IF EXISTS dim_trip;
            CREATE TABLE dim_trip AS
            SELECT
                trip_id AS id,
                trip_headsign AS headsign,
                trip_short_name AS short_name,
                direction_id
            FROM '{os.environ["WORK_DIR"]}/data/schedule/trips.csv';
            ALTER TABLE dim_trip ADD PRIMARY KEY (id)
            """
        conn.execute(sql_query)
        print(conn.sql("SELECT COUNT(*) FROM dim_trip").fetchone()[0])
        conn.close()

    @task(task_id="create_dim_time_table")
    def t6_4():
        """
        Create the dim_time table in DuckDB.
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # execute a simple query
        sql_query = """
            CREATE TABLE IF NOT EXISTS dim_time (
                event_ts TIMESTAMP WITH TIME ZONE NOT NULL,
                date date NOT NULL,
                year int NOT NULL,
                month int NOT NULL,
                day_of_month int NOT NULL,
                week int NOT NULL,
                day_of_week int NOT NULL,
                time time NOT NULL,
                hour int NOT NULL,
                minute int NOT NULL,
                PRIMARY KEY (event_ts)
            )
            """
        conn.execute(sql_query)
        print(conn.sql("SELECT COUNT(*) FROM dim_time").fetchone()[0])
        conn.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    t7 = BashOperator(
        task_id="remove_txt_files",
        bash_command=f"""
        cd {os.environ["WORK_DIR"]}/data/schedule_temp;
        rm *.txt
        """,
        trigger_rule="all_done",
    )

    t0 >> t1 >> t2
    s3_1 >> t4_1 >> t5_1 >> t6_1() >> t7
    s3_2 >> t4_2 >> t5_2 >> t6_2() >> t7
    s3_3 >> t4_3 >> t5_3 >> t6_3() >> t7
    s3_4 >> t4_4 >> t5_4 >> t7
    t6_4() >> t7
