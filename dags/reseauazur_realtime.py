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

        return vehicle_positions

    vehicle_positions = download_vehicle_positions()


process_realtime_data()
