import pendulum
from airflow.exceptions import AirflowException
import numpy as np
import pandas as pd


def convert_timestamp_into_france_datetime(row, key: str):
    if isinstance(row[key], int):
        return pendulum.from_timestamp(row[key], tz="Europe/Paris")
    else:
        return None


def convert_scheduled_time_into_france_datetime(
    row, key: str, service_year: int, service_month: int, service_day: int
):
    if row[key]:
        arrival_hour = int(row[key][0:2])
        if arrival_hour < 24:
            return pendulum.parse(row[key], tz="Europe/Paris").on(
                service_year,
                service_month,
                service_day,
            )
        else:
            raise AirflowException("24:00:00+ problem not handled yet")
    else:
        return None


def compute_time_offset(row, key_scheduled: str, key_real: str) -> int | None:
    if not pd.isnull(row[key_scheduled]) and not pd.isnull(row[key_real]):
        scheduled_arrival_time = pendulum.instance(row[key_scheduled])
        real_arrival_time = pendulum.instance(row[key_real])
        return scheduled_arrival_time.diff(real_arrival_time, False).in_seconds()
    else:
        return None



def derive_on_time_status(row, key_time_offset: str) -> int | None:
    if not pd.isnull(row[key_time_offset]):
        if row[key_time_offset] <= 120:
            return 0
        elif row[key_time_offset] <= 300:
            return 1
        else:
            return 9
    else:
        return -1


def get_service_date() -> tuple[int, int, int]:
    """Get service year, month and day
    Between 0:00 and 3:00 we consider the service date
    to be still the previous day. In other words, change
    of service is done at 3:00.

    Returns:
        tuple[int, int, int]: Year, month and day of the service date
    """
    now_datetime = pendulum.now()

    # TODO: REMOVE: only for current test
    now_datetime = now_datetime.subtract(days=1)

    # Change of service date at 3:00
    if 0 <= now_datetime.hour <= 3:
        service_datetime = now_datetime.subtract(days=1)
    else:
        service_datetime = now_datetime

    return (service_datetime.year, service_datetime.month, service_datetime.day)
