import os
import pendulum
import pandas as pd
import requests

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
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
    schedule="0 12 * * *",
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
    def download_vehicle_positions() -> pd.DataFrame:
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

                if entity.vehicle.HasField("stop_id"):
                    stop_id = entity.vehicle.stop_id
                else:
                    raise AirflowException("Missing field vehicle.stop_id")

                if entity.vehicle.HasField("vehicle"):
                    if entity.vehicle.vehicle.HasField("id"):
                        vehicle_id = entity.vehicle.vehicle.id
                    else:
                        raise AirflowException("Missing field vehicle.vehicle.id")
                else:
                    raise AirflowException("Missing field vehicle.vehicle")

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
            f"{os.environ["WORK_DIR"]}/data/realtime/vehicule_positions.csv",
            index=False,
        )
        return vehicle_positions

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
        return trip_updates

    _ = download_vehicle_positions()
    _ = download_trip_updates()


process_realtime_data()
