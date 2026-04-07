import os
import json
import pm4py
import pandas as pd
import numpy as np


def run_variants_once_median_throughput_benchmark(event_log, output_dir="output", unit="seconds"):
    if event_log is None:
        raise ValueError("event_log is None. Expected a PM4Py EventLog.")
    if not hasattr(event_log, "__iter__"):
        raise TypeError("Invalid event_log type. Expected an iterable PM4Py EventLog.")

    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])

    # Calculate throughput time for each case
    log_df["throughput_time"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())

    # Get the variants and their counts
    variant_counts = log_df["case:concept:name"].value_counts()
    unique_variants = variant_counts[variant_counts == 1].index.tolist()

    # Filter the log for unique variants
    unique_cases_df = log_df[log_df["case:concept:name"].isin(unique_variants)]

    # Calculate the median throughput time for unique variants
    median_throughput_time = np.median(unique_cases_df["throughput_time"]) if not unique_cases_df.empty else None

    # Prepare the final answer
    final_answer = {"median_throughput_time": median_throughput_time}

    # Save the final answer to a JSON file
    with open(os.path.join(output_dir, "median_throughput_time.json"), "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'median_throughput_time.json')}")

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


def main():
    event_log = ACTIVE_LOG
    run_variants_once_median_throughput_benchmark(event_log)