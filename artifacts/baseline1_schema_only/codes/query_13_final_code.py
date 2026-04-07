import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Calculate throughput times
    log_df["throughput_time"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: x.max() - x.min()).dt.total_seconds() / 3600  # Convert to hours
    average_throughput_time = log_df["throughput_time"].mean()
    median_throughput_time = log_df["throughput_time"].median()
    final_answer = {"average_throughput_time": average_throughput_time, "median_throughput_time": median_throughput_time}
    # Save the final answer to a JSON file
    output_path = "output/throughput_times.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {output_path}")
    print(json.dumps(final_answer, ensure_ascii=False))