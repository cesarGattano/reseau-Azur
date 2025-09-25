import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from utils.utils import ranged_values_to_colors_3p
import plotly.graph_objects as go

# Vehicle with position time stamp too old
st.title("Vehicles with old positions")
st.text("Limit to data from last RT extraction.")
st.text("Show only vehicles whose position is older than 15 minutes.")

df = pd.read_csv("vehicle_ts_outliers.csv")
st.write(df)
st.bar_chart(
    df,
    x="vehicle_id",
    y="ts_diff_in_min",
    x_label="Vehicle identifiers",
    y_label="timestamp difference [min]",
)

# Total average delay during the current day
st.title("Instantaneous total average delay")
st.text("Data aggregated over the extraction timestamp.")
st.text(
    "Some trip updates show negative delay. This explained why some average values are negative."
)
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("total_average_delay_wrt_time.csv")
st.write(df)
st.line_chart(
    df,
    x="xtr_ts",
    y="avg_arrival_time_offset",
    x_label="extraction time",
    y_label="average delays [seconds]",
)

# Vehicle instantenous positions with their delays
st.title("Instantaneous vehicle with delays")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("vehicle_positions_with_delay.csv")
st.write(df)
df["color"] = df.apply(
    ranged_values_to_colors_3p,
    axis=1,
    key="arrival_time_offset",
    b_ceil=-60,
    g_origin=0,
    r_ceil=300,
)
st.map(df, latitude="latitude", longitude="longitude", color="color")

# Station positions with their average delays from approaching vehicles
st.title("Station with average delays")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
st.text("Stops are aggregated over their parent station.")
df = pd.read_csv("station_positions_with_avg_delay.csv")
st.write(df)
df["color"] = df.apply(
    ranged_values_to_colors_3p,
    axis=1,
    key="avg_arrival_time_offset",
    b_ceil=-60,
    g_origin=0,
    r_ceil=300,
)
st.map(df, latitude="latitude", longitude="longitude", color="color")

# Station positions with their average delays from approaching vehicles
st.title("Routes with total average delays")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("route_with_tot_avg_delay.csv")
st.write(df)
st.bar_chart(
    df,
    x="name",
    y="avg_departure_time_offset",
    x_label="Routes",
    y_label="departure_time_offset",
    color="color",
    horizontal=True,
)

# Average delays with respect ot the day of week and time
st.title("Average delay per day of week and time")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("avg_delay_per_dow_and_time.csv")
df["time"] = pd.to_datetime(df["time"])
df["ftime"] = (
    df["time"].dt.hour + df["time"].dt.minute / 60 + df["time"].dt.second / 3600
)
df = df.round(decimals=2)
st.write(df)
hm = df.pivot(index="ftime", columns="day_of_week", values="avg_arrival_time_offset")
st.write(hm)
sns.heatmap(
    hm,
    center=0.0,
    annot=False,
    cmap="coolwarm",
)
st.pyplot(plt)

# Number of on_time bus
st.title("Gauge of on-time bus")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("nb_on_time_vehicle.csv")
nb_vehicle = int(df.at[0, "nb_vehicle"])
nb_on_time_vehicle = int(df.at[0, "nb_on_time_vehicle"])
nb_nearly_on_time_vehicle = int(df.at[0, "nb_nearly_on_time_vehicle"])
fig = go.Figure(
    go.Indicator(
        mode="gauge+number",
        value=nb_on_time_vehicle + nb_nearly_on_time_vehicle,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "Speed"},
        gauge={"axis": {"range": [None, nb_vehicle]}},
    )
)
st.plotly_chart(fig)

# Evolution of delay for one stop
st.title("Evolution of the delay for the stop")
st.text("Vehicles whose positions are older than 15 minutes are excluded.")
df = pd.read_csv("delay_evolution_per_stop.csv")
st.line_chart(df, x="scheduled_arrival_time", y="median_arrival_time_offset")
