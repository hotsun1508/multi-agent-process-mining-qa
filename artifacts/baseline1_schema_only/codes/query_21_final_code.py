import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Identify the most frequent variant
    variant_counts = log_df.groupby("case:concept:name")["concept:name"].apply(lambda x: tuple(x)).value_counts()
    most_frequent_variant = variant_counts.idxmax()
    
    # Filter cases that follow the most frequent variant
    filtered_cases = log_df[log_df.groupby("case:concept:name")["concept:name"].transform(lambda x: tuple(x) == most_frequent_variant)]
    
    # Calculate throughput time for these cases
    filtered_cases["time:timestamp"] = pd.to_datetime(filtered_cases["time:timestamp"])
    throughput_times = filtered_cases.groupby("case:concept:name").agg(start_time=("time:timestamp", "min"), end_time=("time:timestamp", "max"))
    throughput_times["throughput_time"] = (throughput_times["end_time"] - throughput_times["start_time"]).dt.total_seconds() / 3600  # Convert to hours
    average_throughput_time = throughput_times["throughput_time"].mean()
    
    # Prepare final answer
    final_answer = {"average_throughput_time_hours": average_throughput_time}
    
    # Save the final answer to a JSON file
    output_path = "output/average_throughput_time.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {output_path}")
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))