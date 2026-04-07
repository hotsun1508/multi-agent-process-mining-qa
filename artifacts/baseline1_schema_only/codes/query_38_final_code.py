import pm4py
import pandas as pd
import json
import os
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Step 1: Identify top 5 resources by event frequency
    top_resources = log_df["org:resource"].value_counts().head(5).index.tolist()
    
    # Step 2: Filter cases involving at least one of the top resources
    filtered_cases = log_df[log_df["org:resource"].isin(top_resources)]
    
    # Step 3: Calculate total case duration and overall average case duration
    case_durations = filtered_cases.groupby("case:concept:name")["time:timestamp"].agg(["min", "max"])
    case_durations["duration"] = (case_durations["max"] - case_durations["min"]).dt.total_seconds()
    overall_avg_duration = case_durations["duration"].mean()
    
    # Step 4: Find cases whose total case duration exceeds the overall average case duration
    delayed_cases = case_durations[case_durations["duration"] > overall_avg_duration].index.tolist()
    delayed_cases_df = filtered_cases[filtered_cases["case:concept:name"].isin(delayed_cases)]
    
    # Step 5: Report the dominant variant among those delayed cases
    dominant_variant = delayed_cases_df.groupby(["case:concept:name", "concept:name"]).size().idxmax()[0]
    
    # Prepare final answer
    final_answer = {
        "top_resources": top_resources,
        "overall_avg_duration": overall_avg_duration,
        "delayed_cases_count": len(delayed_cases),
        "dominant_variant": dominant_variant
    }
    
    # Save final answer to JSON
    output_path = "output/final_answer.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: {output_path}")
    
    # Print final answer
    print(json.dumps(final_answer, ensure_ascii=False))