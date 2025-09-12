import os
import pendulum
from datetime import timedelta
import pandas as pd
import requests
from utils.utils import (
    convert_timestamp_into_france_datetime,
    convert_scheduled_time_into_france_datetime,
    compute_time_offset,
    derive_on_time_status,
    get_service_date,
    get_insert_vehicle_ts_in_dim_time_query,
    route_id_match_in_dim_route,
)

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

load_dotenv()


@dag(
    dag_id="collect_process_store_realtime_data",
    description="""
    Download the gtfs realtime data of the Reseau Azur Transport at Nice,
    clean them, process them in order to compute the delay for each bus
    and each of their incoming stops until terminus,
    and store them in the duckDB database.
    """,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2025, 9, 5),
    end_date=pendulum.datetime(2025, 9, 30),
    default_args={"retries": 1},
    tags=["reseauazur"],
    max_active_runs=1,
)
def process_realtime_data():
    """
    ### GTFS-realtime Reseau Azur data pipeline documentation
    This is a data pipeline to download the GTFS-realtime
    Reaseau Azur data from the platform data.gouv.fr, clean
    them, process them in order to prepare an OLAP database
    that will serve for the creation of a dashboard to
    monitor survey in the Reseau Azur network at Nice.
    """

    @task(task_id="download_vehicle_positions", retries=0)
    def download_vehicle_positions():
        """
        #### Download the vehicle positions
        ...
        """

        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(os.environ["GTFS_REALTIME_VP_URL"])
        feed.ParseFromString(response.content)
        print(feed.header)

        # Initialize the dataframe
        vehicle_positions = pd.DataFrame(
            columns=[
                "trip_id",
                "route_id",
                "latitude",
                "longitude",
                "timestamp",
                "stop_id",
                "vehicle_id",
            ]
        )

        for entity in feed.entity:
            if entity.HasField("vehicle"):
                if entity.vehicle.HasField("trip"):
                    if entity.vehicle.trip.HasField("trip_id"):
                        trip_id = entity.vehicle.trip.trip_id
                    else:
                        raise AirflowException("Missing field vehicle.trip.trip_id")
                    if entity.vehicle.trip.HasField("route_id"):
                        route_id = entity.vehicle.trip.route_id
                    else:
                        raise AirflowException("Missing field vehicle.trip.route_id")
                else:
                    raise AirflowException("Missing field vehicle.trip")

                if entity.vehicle.HasField("position"):
                    latitude = entity.vehicle.position.latitude
                    longitude = entity.vehicle.position.longitude
                else:
                    raise AirflowException("Missing field vehicle.position")

                if entity.vehicle.HasField("timestamp"):
                    timestamp = entity.vehicle.timestamp
                else:
                    raise AirflowException("Missing field vehicle.timestamp")

                if entity.vehicle.HasField("vehicle"):
                    if entity.vehicle.vehicle.HasField("id"):
                        vehicle_id = entity.vehicle.vehicle.id
                    else:
                        raise AirflowException("Missing field vehicle.vehicle.id")
                else:
                    raise AirflowException("Missing field vehicle.vehicle")

                if entity.vehicle.HasField("stop_id"):
                    stop_id = entity.vehicle.stop_id
                else:
                    # TODO: why ? investigate...
                    print("Missing field vehicle.stop_id")
                    print("Drop entry with vehicle_id: ", vehicle_id)
                    continue
                    # raise AirflowException("Missing field vehicle.stop_id")

                vehicle_positions.loc[entity.id] = pd.Series(
                    {
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "latitude": latitude,
                        "longitude": longitude,
                        "timestamp": timestamp,
                        "stop_id": stop_id,
                        "vehicle_id": vehicle_id,
                    }
                )
            else:
                raise AirflowException("Missing field vehicle")

        vehicle_positions.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions.csv",
            index=False,
        )

    @task(
        task_id="extract_vehicle_scheduled_stop_times",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def extract_vehicle_scheduled_stop_times():
        """
        #### Extract the scheduled stop times for each vehicule
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # TODO: INNER JOIN may exlcude some vehicles because
        # their trip_id+stop_id do not find match in stop_times.txt
        # Investigate...
        sql_query = f"""
            SELECT
                vp.vehicle_id AS vehicle_id,
                vp.trip_id AS trip_id,
                vp.stop_id AS stop_id,
                st.stop_sequence AS stop_sequence
            FROM '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions.csv' AS vp
            INNER JOIN
                read_csv('{os.environ["WORK_DIR"]}/data/schedule/stop_times.csv') AS st
            ON
                vp.trip_id = st.trip_id
                AND vp.stop_id = st.stop_id;
        """
        vehicle_positions = conn.execute(sql_query).df()

        vehicle_positions.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_next_stops.csv",
            index=False,
        )

        sql_query = f"""
            SELECT
                vp.vehicle_id AS vehicle_id,
                vp.timestamp AS vehicle_ts,
                vp.longitude AS longitude,
                vp.latitude AS latitude,
                vp.trip_id AS trip_id,
                vp.route_id AS route_id,
                st.stop_id AS stop_id,
                st.stop_sequence AS stop_sequence,
                st.arrival_time AS scheduled_arrival_time,
                st.departure_time AS scheduled_departure_time
            FROM '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions.csv' AS vp
            INNER JOIN
                read_csv('{os.environ["WORK_DIR"]}/data/schedule/stop_times.csv',
                    types = {{'arrival_time': 'VARCHAR', 'departure_time': 'VARCHAR'}}
                ) AS st
            ON
                vp.trip_id = st.trip_id
            INNER JOIN '{os.environ["WORK_DIR"]}/data/realtime/vehicle_next_stops.csv' AS ns
            ON
                vp.vehicle_id = ns.vehicle_id
        """
        vehicle_positions = conn.execute(sql_query).df()
        conn.close()

        vehicle_positions.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_compl.csv",
            index=False,
        )

    @task(task_id="convert_vehicle_times", retries=0)
    def compute_vehicle_times():
        """
        #### Convert the times in the vehicle_positions
        ...
        """

        service_year, service_month, service_day = get_service_date()

        # Collect
        vehicle_positions = pd.read_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_compl.csv",
            keep_default_na=False,
            dtype={"vehicle_ts": "Int64"},
        )
        # Convert
        key = "vehicle_ts"
        vehicle_positions["vehicle_ts"] = vehicle_positions.apply(
            convert_timestamp_into_france_datetime, axis=1, key="vehicle_ts"
        )
        vehicle_positions["scheduled_arrival_time"] = vehicle_positions.apply(
            convert_scheduled_time_into_france_datetime,
            axis=1,
            args=("scheduled_arrival_time", service_year, service_month, service_day),
        )
        vehicle_positions["scheduled_departure_time"] = vehicle_positions.apply(
            convert_scheduled_time_into_france_datetime,
            axis=1,
            args=("scheduled_departure_time", service_year, service_month, service_day),
        )
        # Store
        vehicle_positions.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_datetime.csv",
            index=False,
        )

    @task(task_id="download_trip_updates", retries=0)
    def download_trip_updates() -> pd.DataFrame:
        """
        #### Download the trip updates
        ...
        """

        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(os.environ["GTFS_REALTIME_TU_URL"])
        feed.ParseFromString(response.content)
        print(feed.header)

        trip_updates = pd.DataFrame(
            columns=[
                "trip_id",
                "route_id",
                "stop_id",
                "stop_sequence",
                "arrival_time",
                "departure_time",
            ]
        )

        missing_arrival_time_count = 0
        missing_departure_time_count = 0
        for entity in feed.entity:
            if entity.HasField("trip_update"):
                if entity.trip_update.HasField("trip"):
                    if entity.trip_update.trip.HasField("trip_id"):
                        trip_id = entity.trip_update.trip.trip_id
                    else:
                        raise AirflowException("Missing field trip_update.trip.trip_id")
                    if entity.trip_update.trip.HasField("route_id"):
                        route_id = entity.trip_update.trip.route_id
                    else:
                        raise AirflowException(
                            "Missing field trip_update.trip.route_id"
                        )
                else:
                    raise AirflowException("Missing field trip_update.trip")

                for stop_time_update in entity.trip_update.stop_time_update:
                    if stop_time_update.HasField("stop_sequence"):
                        stop_sequence = stop_time_update.stop_sequence
                    else:
                        raise AirflowException(
                            "Missing field trip_update.stop_time_update.stop_sequence"
                        )

                    if stop_time_update.HasField("arrival"):
                        if stop_time_update.arrival.HasField("time"):
                            arrival_time = stop_time_update.arrival.time
                        else:
                            missing_arrival_time_count += 1
                            arrival_time = None
                    else:
                        raise AirflowException(
                            "Missing field trip_update.stop_time_update.arrival"
                        )

                    if stop_time_update.HasField("departure"):
                        if stop_time_update.departure.HasField("time"):
                            departure_time = stop_time_update.departure.time
                        else:
                            missing_departure_time_count += 1
                            departure_time = None
                    else:
                        raise AirflowException(
                            "Missing field trip_update.stop_time_update.departure"
                        )

                    if stop_time_update.HasField("stop_id"):
                        stop_id = stop_time_update.stop_id
                    else:
                        raise AirflowException(
                            "Missing field trip_update.stop_time_update.stop_id"
                        )

                    trip_updates.loc[
                        entity.id + str(stop_time_update.stop_sequence)
                    ] = pd.Series(
                        {
                            "trip_id": trip_id,
                            "route_id": route_id,
                            "stop_id": stop_id,
                            "stop_sequence": stop_sequence,
                            "arrival_time": arrival_time,
                            "departure_time": departure_time,
                        }
                    )

            else:
                raise AirflowException("Missing field trip_update")

        trip_updates.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/trip_updates.csv",
            index=False,
        )

    @task(
        task_id="extract_scheduled_stop_times",
        retries=2,
        retry_delay=timedelta(seconds=6),
    )
    def extract_scheduled_stop_times():
        """
        #### Extract the schedules stop times
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # TODO: check known errors from transport.data.gouv.fr
        sql_query = f"""
            SELECT
                tu.trip_id AS trip_id,
                tu.route_id AS route_id,
                tu.stop_id AS stop_id,
                tu.stop_sequence AS stop_sequence,
                tu.arrival_time AS real_arrival_time,
                tu.departure_time AS real_departure_time,
                st.arrival_time AS scheduled_arrival_time,
                st.departure_time AS scheduled_departure_time
            FROM '{os.environ["WORK_DIR"]}/data/realtime/trip_updates.csv' AS tu
            LEFT JOIN
                read_csv('{os.environ["WORK_DIR"]}/data/schedule/stop_times.csv',
                    types = {{'arrival_time': 'VARCHAR', 'departure_time': 'VARCHAR'}}
                ) AS st
            ON
                tu.trip_id = st.trip_id
                AND tu.stop_id = st.stop_id
                AND tu.stop_sequence = st.stop_sequence;
        """
        trip_updates = conn.execute(sql_query).df()
        conn.close()

        trip_updates.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/trip_updates_compl.csv",
            index=False,
        )

    @task(task_id="compute_realtime_delays", retries=0)
    def compute_realtime_delays():
        """
        #### Compute the realtime delays for all stops
        ...
        """

        service_year, service_month, service_day = get_service_date()

        # Collect
        trip_updates = pd.read_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/trip_updates_compl.csv",
            keep_default_na=False,
            dtype={"real_arrival_time": "Int64", "real_departure_time": "Int64"},
        )
        # Convert
        trip_updates["real_arrival_time"] = trip_updates.apply(
            convert_timestamp_into_france_datetime, axis=1, key="real_arrival_time"
        )
        trip_updates["real_departure_time"] = trip_updates.apply(
            convert_timestamp_into_france_datetime, axis=1, key="real_departure_time"
        )
        trip_updates["scheduled_arrival_time"] = trip_updates.apply(
            convert_scheduled_time_into_france_datetime,
            axis=1,
            args=("scheduled_arrival_time", service_year, service_month, service_day),
        )
        trip_updates["scheduled_departure_time"] = trip_updates.apply(
            convert_scheduled_time_into_france_datetime,
            axis=1,
            args=("scheduled_departure_time", service_year, service_month, service_day),
        )
        # Append
        trip_updates["arrival_time_offset"] = trip_updates.apply(
            compute_time_offset,
            axis=1,
            key_scheduled="scheduled_arrival_time",
            key_real="real_arrival_time",
        )
        trip_updates["departure_time_offset"] = trip_updates.apply(
            compute_time_offset,
            axis=1,
            key_scheduled="scheduled_departure_time",
            key_real="real_departure_time",
        )
        trip_updates["arrival_on_time_status"] = trip_updates.apply(
            derive_on_time_status, axis=1, key_time_offset="arrival_time_offset"
        )
        trip_updates["departure_on_time_status"] = trip_updates.apply(
            derive_on_time_status, axis=1, key_time_offset="departure_time_offset"
        )
        # Store
        trip_updates.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/trip_updates_with_delays.csv",
            index=False,
        )

    @task(task_id="join_trip_updates_and_vehicle_positions", retries=0)
    def join_trip_updates_and_vehicle_positions():
        """
        #### Join the realtime data together
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        tot_vehicle_entries = conn.sql(
            f"""
            SELECT COUNT(*) FROM '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_datetime.csv'
            """
        ).fetchone()[0]

        # TODO: check known errors from transport.data.gouv.fr
        sql_query = f"""
            SELECT
                vp.vehicle_id AS vehicle_id,
                vp.vehicle_ts AS vehicle_ts,
                vp.trip_id AS trip_id,
                vp.route_id AS route_id,
                vp.stop_id AS stop_id,
                vp.stop_sequence AS stop_sequence,
                vp.longitude AS longitude,
                vp.latitude AS latitude,
                tu.real_arrival_time AS real_arrival_time,
                tu.real_departure_time AS real_departure_time,
                tu.scheduled_arrival_time AS scheduled_arrival_time,
                tu.scheduled_departure_time AS scheduled_departure_time,
                tu.arrival_time_offset AS arrival_time_offset,
                tu.departure_time_offset AS departure_time_offset,
                tu.arrival_on_time_status AS arrival_on_time_status,
                tu.departure_on_time_status AS departure_on_time_status
            FROM '{os.environ["WORK_DIR"]}/data/realtime/trip_updates_with_delays.csv' AS tu
            INNER JOIN '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_datetime.csv' AS vp
            ON tu.trip_id = vp.trip_id
                AND tu.stop_id = vp.stop_id
                AND tu.stop_sequence = vp.stop_sequence
                AND tu.scheduled_arrival_time = vp.scheduled_arrival_time
                AND tu.scheduled_departure_time = vp.scheduled_departure_time;
        """
        vehicle_positions_with_trip_updates = conn.execute(sql_query).df()

        vehicle_positions_with_trip_updates.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_trip_updates.csv",
            index=False,
        )

        n_vehicle_with_trip_updates = conn.sql(
            f"""
            SELECT COUNT(*) FROM '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_trip_updates.csv'
            """
        ).fetchone()[0]

        # TODO: check known errors from transport.data.gouv.fr
        sql_query = f"""
        SELECT
            vp.vehicle_id AS vehicle_id,
            vp.vehicle_ts AS vehicle_ts,
            vp.trip_id AS trip_id,
            vp.route_id AS route_id,
            vp.stop_id AS stop_id,
            vp.stop_sequence AS stop_sequence,
            vp.longitude AS longitude,
            vp.latitude AS latitude,
            vp.scheduled_arrival_time AS scheduled_arrival_time,
            vp.scheduled_departure_time AS scheduled_departure_time
        FROM '{os.environ["WORK_DIR"]}/data/realtime/trip_updates_with_delays.csv' AS tu
        RIGHT JOIN '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_datetime.csv' AS vp
        ON tu.trip_id = vp.trip_id
            AND tu.stop_id = vp.stop_id
            AND tu.stop_sequence = vp.stop_sequence
        WHERE
            tu.trip_id IS NULL
            AND tu.stop_id IS NULL
            AND tu.stop_sequence IS NULL;
        """
        vehicle_positions_without_trip_updates = conn.execute(sql_query).df()

        vehicle_positions_without_trip_updates.to_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_without_trip_updates.csv",
            index=False,
        )

        n_vehicle_without_trip_updates = conn.sql(
            f"""
            SELECT COUNT(*) FROM '{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_without_trip_updates.csv'
            """
        ).fetchone()[0]

        conn.close()

        print("Total vehicle entries:    ", tot_vehicle_entries)
        print("... with trip updates:    ", n_vehicle_with_trip_updates)
        print("... without trip updates: ", n_vehicle_without_trip_updates)
        print(
            "Lost entries...:          ",
            tot_vehicle_entries
            - (n_vehicle_without_trip_updates + n_vehicle_with_trip_updates),
        )

    @task(task_id="create_table_vehicle_next_stop_times", retries=0)
    def create_table_vehicle_next_stop_times():
        """
        #### Create the fact table
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        sql_query = """
            CREATE SEQUENCE IF NOT EXISTS position_id START 1;
        """
        conn.execute(sql_query)

        sql_query = """
            CREATE TABLE IF NOT EXISTS vehicle_next_stop_times (
                position_id INTEGER NOT NULL DEFAULT nextval('position_id'),
                vehicle_ts TIMESTAMP WITH TIME ZONE NOT NULL REFERENCES dim_time(event_ts),
                stop_id VARCHAR NOT NULL REFERENCES dim_stop(id),
                trip_id VARCHAR NOT NULL REFERENCES dim_trip(id),
                route_id VARCHAR NOT NULL REFERENCES dim_route(id),
                stop_sequence INTEGER NOT NULL,
                vehicle_id VARCHAR NOT NULL,
                scheduled_arrival_time TIMESTAMP WITH TIME ZONE NOT NULL,
                scheduled_departure_time TIMESTAMP WITH TIME ZONE NOT NULL,
                real_arrival_time TIMESTAMP WITH TIME ZONE,
                real_departure_time TIMESTAMP WITH TIME ZONE,
                arrival_time_offset INTEGER,
                departure_time_offset INTEGER,
                arrival_on_time_status INTEGER NOT NULL,
                departure_on_time_status INTEGER NOT NULL,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                PRIMARY KEY (position_id)
            );
        """
        conn.execute(sql_query)
        print(conn.sql("SELECT COUNT(*) FROM vehicle_next_stop_times").fetchone()[0])
        conn.close()

    @task(task_id="store_vehicle_positions_with_trip_updates", retries=0)
    def store_vehicle_positions_with_trip_updates():
        """
        #### Store vehicle positions with a trip updates
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # Collect
        vehicule_positions = pd.read_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_with_trip_updates.csv",
            keep_default_na=False,
        )

        for _, row in vehicule_positions.iterrows():

            # Insert vehicle timestamp in dim_time if not exist
            sql_query = get_insert_vehicle_ts_in_dim_time_query(row)
            conn.execute(sql_query)

            # Known error from data.gouv.fr:
            # Check if roue_id is present in gtfs schedule file routes.txt
            if not route_id_match_in_dim_route(row, conn):
                row["route_id"] = "Unknown"

            # Insert data into table vehicle_next_stop_times
            sql_query = "INSERT OR IGNORE INTO vehicle_next_stop_times (\n"
            sql_query += "    vehicle_ts,\n"
            sql_query += "    stop_id,\n"
            sql_query += "    trip_id,\n"
            sql_query += "    route_id,\n"
            sql_query += "    stop_sequence,\n"
            sql_query += "    vehicle_id,\n"
            sql_query += "    scheduled_arrival_time,\n"
            sql_query += "    scheduled_departure_time,\n"
            if row["real_arrival_time"]:
                sql_query += "    real_arrival_time,\n"
            if row["real_departure_time"]:
                sql_query += "    real_departure_time,\n"
            if row["arrival_time_offset"]:
                sql_query += "    arrival_time_offset,\n"
            if row["departure_time_offset"]:
                sql_query += "    departure_time_offset,\n"
            sql_query += "    arrival_on_time_status,\n"
            sql_query += "    departure_on_time_status,\n"
            sql_query += "    latitude,\n"
            sql_query += "    longitude\n"
            sql_query += ") VALUES (\n"
            sql_query += f"    '{row["vehicle_ts"]}',\n"
            sql_query += f"    '{row["stop_id"]}',\n"
            sql_query += f"    '{row["trip_id"]}',\n"
            sql_query += f"    '{row["route_id"]}',\n"
            sql_query += f"    {row["stop_sequence"]},\n"
            sql_query += f"    '{row["vehicle_id"]}',\n"
            sql_query += f"    '{row["scheduled_arrival_time"]}',\n"
            sql_query += f"    '{row["scheduled_departure_time"]}',\n"
            if row["real_arrival_time"]:
                sql_query += f"    '{row["real_arrival_time"]}',\n"
            if row["real_departure_time"]:
                sql_query += f"    '{row["real_departure_time"]}',\n"
            if row["arrival_time_offset"]:
                sql_query += f"    {row["arrival_time_offset"]},\n"
            if row["departure_time_offset"]:
                sql_query += f"    {row["departure_time_offset"]},\n"
            sql_query += f"    {row["arrival_on_time_status"]},\n"
            sql_query += f"    {row["departure_on_time_status"]},\n"
            sql_query += f"    {row["latitude"]},\n"
            sql_query += f"    {row["longitude"]}\n"
            sql_query += ");"
            conn.execute(sql_query)

        print(conn.sql("SELECT COUNT(*) FROM dim_time").fetchone()[0])
        conn.close()

    @task(task_id="store_vehicle_positions_without_trip_updates", retries=0)
    def store_vehicle_positions_without_trip_updates():
        """
        #### Store vehicle positions without a trip updates
        Consider them to be following the scheduled time
        ...
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()

        # Collect
        vehicule_positions = pd.read_csv(
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicle_positions_without_trip_updates.csv",
            keep_default_na=False,
        )

        for _, row in vehicule_positions.iterrows():

            # Insert vehicle timestamp in dim_time if not exist
            sql_query = get_insert_vehicle_ts_in_dim_time_query(row)
            conn.execute(sql_query)

            # Known error from data.gouv.fr:
            # Check if route_id is present in gtfs schedule file routes.txt
            if not route_id_match_in_dim_route(row, conn):
                row["route_id"] = "Unknown"

            # Insert data into table vehicle_next_stop_times
            sql_query = "INSERT OR IGNORE INTO vehicle_next_stop_times (\n"
            sql_query += "    vehicle_ts,\n"
            sql_query += "    stop_id,\n"
            sql_query += "    trip_id,\n"
            sql_query += "    route_id,\n"
            sql_query += "    stop_sequence,\n"
            sql_query += "    vehicle_id,\n"
            sql_query += "    scheduled_arrival_time,\n"
            sql_query += "    scheduled_departure_time,\n"
            sql_query += "    real_arrival_time,\n"
            sql_query += "    real_departure_time,\n"
            sql_query += "    arrival_time_offset,\n"
            sql_query += "    departure_time_offset,\n"
            sql_query += "    arrival_on_time_status,\n"
            sql_query += "    departure_on_time_status,\n"
            sql_query += "    latitude,\n"
            sql_query += "    longitude\n"
            sql_query += ") VALUES (\n"
            sql_query += f"    '{row["vehicle_ts"]}',\n"
            sql_query += f"    '{row["stop_id"]}',\n"
            sql_query += f"    '{row["trip_id"]}',\n"
            sql_query += f"    '{row["route_id"]}',\n"
            sql_query += f"    {row["stop_sequence"]},\n"
            sql_query += f"    '{row["vehicle_id"]}',\n"
            sql_query += f"    '{row["scheduled_arrival_time"]}',\n"
            sql_query += f"    '{row["scheduled_departure_time"]}',\n"
            sql_query += f"    '{row["scheduled_arrival_time"]}',\n"
            sql_query += f"    '{row["scheduled_departure_time"]}',\n"
            sql_query += "    0,\n"
            sql_query += "    0,\n"
            sql_query += "    0,\n"
            sql_query += "    0,\n"
            sql_query += f"    {row["latitude"]},\n"
            sql_query += f"    {row["longitude"]}\n"
            sql_query += ");"
            print(sql_query)
            conn.execute(sql_query)

        print(conn.sql("SELECT COUNT(*) FROM dim_time").fetchone()[0])
        conn.close()

    connector = EmptyOperator(task_id="connector")

    (
        download_trip_updates()
        >> extract_scheduled_stop_times()
        >> compute_realtime_delays()
        >> connector
    )
    (
        download_vehicle_positions()
        >> extract_vehicle_scheduled_stop_times()
        >> compute_vehicle_times()
        >> connector
        >> join_trip_updates_and_vehicle_positions()
        >> create_table_vehicle_next_stop_times()
        >> store_vehicle_positions_with_trip_updates()
        >> store_vehicle_positions_without_trip_updates()
    )
    # (extract_scheduled_stop_times() >> compute_realtime_delays() >> connector)
    # (
    #     extract_vehicle_scheduled_stop_times()
    #     >> compute_vehicle_times()
    #     >> connector
    #     >> join_trip_updates_and_vehicle_positions()
    #     >> create_table_vehicle_next_stop_times()
    #     >> store_vehicle_positions_with_trip_updates()
    #     >> store_vehicle_positions_without_trip_updates()
    # )


process_realtime_data()
