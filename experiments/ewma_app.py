import math

import altair as alt
import pandas as pd
import streamlit as st

cpu_usage = [
    1.9, 6.4, 1.3, 10.6, 1.9, 13.9, 1.2, 15.9, 0.6, 21.2,
    7.5, 9.6, 1.2, 5.7, 0.6, 0.0, 8.9, 0.6, 5.0, 3.8,
    5.1, 7.6, 12.1, 5.1, 35.9, 11.0, 31.0, 6.9, 14.7, 4.5,
    6.3, 5.1, 8.2, 10.1, 20.8, 62.8, 7.5, 28.0, 3.8, 21.0,
    13.9, 7.1, 15.1, 10.8, 3.2, 10.2, 5.1, 16.7, 7.0, 8.8,
    46.5, 48.7, 31.2, 20.4, 67.7, 10.1, 14.5, 6.8, 11.2, 8.6,
    28.0, 10.1, 4.4, 14.9, 3.8, 17.8, 9.5, 3.8, 3.9, 15.1,
    15.3, 17.3, 6.4, 3.2, 4.5, 2.6, 13.5, 10.3, 32.1, 6.3,
    18.2, 5.1, 15.4, 4.4, 17.7, 3.2, 27.8, 15.1, 22.2, 5.7,
    6.3, 8.2, 7.0, 9.4, 12.1, 4.4, 12.5, 18.2, 40.1, 7.0,
]

WARMUP_READINGS = 10  # don't flag anything until the variance has had time to build up


def ewma(
    values: list[float],
    alpha: float,
    threshold_multiplier: float,
    min_deviation: float,
    anomaly_damping: float,
) -> tuple[list[float], list[float | None], list[bool]]:
    average = values[0]
    variance = 0.0
    averages = [average]
    thresholds: list[float | None] = [None]
    anomalies = [False]

    for index, value in enumerate(values[1:], start=1):
        # Compare against the previous average: the new one would already contain the spike.
        deviation = value - average
        std_dev = math.sqrt(variance)

        # A reading must beat both the statistical threshold and the minimum absolute jump.
        threshold = average + max(threshold_multiplier * std_dev, min_deviation)
        in_warmup = index < WARMUP_READINGS
        is_anomaly = not in_warmup and value > threshold

        # Anomalies still nudge the average slightly, so it can't get stuck.
        update_alpha = alpha * anomaly_damping if is_anomaly else alpha

        average = average + update_alpha * deviation
        variance = (1 - update_alpha) * (variance + update_alpha * deviation ** 2)

        averages.append(average)
        thresholds.append(None if in_warmup else threshold)
        anomalies.append(is_anomaly)

    return averages, thresholds, anomalies


st.title("EWMA anomaly detection")

with st.sidebar:
    # High alpha -> follows the data closely, noisy. Low alpha -> smooth, reacts slowly.
    alpha = st.slider("Alpha", min_value=0.01, max_value=1.0, value=0.1, step=0.01)
    threshold_multiplier = st.slider(
        "Threshold multiplier (std devs)", min_value=1.0, max_value=5.0, value=3.0, step=0.1
    )
    min_deviation = st.slider(
        "Minimum deviation (percentage points)", min_value=0.0, max_value=30.0, value=10.0, step=0.5
    )
    # 0 = anomalies never update the average (can lock up), 1 = anomalies update it normally.
    anomaly_damping = st.slider(
        "Anomaly damping", min_value=0.0, max_value=1.0, value=0.1, step=0.05
    )

averages, thresholds, anomalies = ewma(
    cpu_usage, alpha, threshold_multiplier, min_deviation, anomaly_damping
)

df = pd.DataFrame(
    {
        "t": range(len(cpu_usage)),
        "cpu_usage": cpu_usage,
        "ewma": averages,
        "threshold": thresholds,
        "anomaly": anomalies,
    }
)

lines = (
    alt.Chart(df.melt("t", ["cpu_usage", "ewma"]))
    .mark_line()
    .encode(x="t:Q", y=alt.Y("value:Q", title=None), color=alt.Color("variable:N", title=None))
)
threshold_line = (
    alt.Chart(df)
    .mark_line(color="gray", strokeDash=[4, 4])
    .encode(x="t:Q", y="threshold:Q", tooltip=["t", alt.Tooltip("threshold:Q", format=".1f")])
)
points = (
    alt.Chart(df[df["anomaly"]])
    .mark_point(color="red", filled=True, size=90)
    .encode(x="t:Q", y="cpu_usage:Q", tooltip=["t", "cpu_usage"])
)

st.altair_chart(lines + threshold_line + points, width="stretch")
st.caption(
    f"Anomalies (red): {int(df['anomaly'].sum())} · "
    f"Dashed gray line: threshold each reading was compared against "
    f"(hidden for the first {WARMUP_READINGS} warm-up readings)"
)

st.subheader("Flagged readings")
st.dataframe(df[df["anomaly"]][["t", "cpu_usage", "ewma", "threshold"]], hide_index=True)