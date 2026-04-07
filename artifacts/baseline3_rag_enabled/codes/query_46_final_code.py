import pm4py
import pandas as pd
import os
import json
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.statistics.variants.log import get as variants_get
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery


def main():
    event_log = ACTIVE_LOG
    # Ensure the output directory exists
    os.makedirs("output", exist_ok=True)

    # Step 1: Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 2: Calculate sojourn time for each event
    log_df['next_timestamp'] = log_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    log_df['sojourn_time'] = (log_df['next_timestamp'] - log_df['time:timestamp']).dt.total_seconds()

    # Step 3: Identify the activity with the longest average sojourn time
    sojourn_times = log_df.groupby('concept:name')['sojourn_time'].mean().dropna()
    longest_avg_activity = sojourn_times.idxmax()
    longest_avg_time = sojourn_times.max()

    # Step 4: Filter the dataframe to include only events related to the identified activity
    activity_df = log_df[log_df['concept:name'] == longest_avg_activity]

    # Step 5: Find the top 5 resources executing the identified activity most frequently
    top_resources = activity_df['org:resource'].value_counts().head(5).index.tolist()

    # Step 6: Filter the original log for cases involving the top resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]

    # Step 7: Determine the dominant variant among the filtered cases
    variants = variants_get.get_variants(filtered_cases)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]

    # Step 8: Discover the Directly-Follows Graph (DFG) from the dominant variant subset
    dfg, start_activities, end_activities = dfg_discovery.apply(filtered_cases[filtered_cases['concept:name'].isin(dominant_variant)])

    # Step 9: Get the top 5 edges from the DFG
    top_edges = sorted(dfg.items(), key=lambda x: x[1], reverse=True)[:5]
    top_edges_summary = [{"source": src, "target": tgt, "count": count} for (src, tgt), count in top_edges]

    # Step 10: Prepare the final result dictionary
    final_answer = {
        "longest_avg_activity": longest_avg_activity,
        "longest_avg_time": longest_avg_time,
        "top_resources": top_resources,
        "dominant_variant": dominant_variant,
        "top_edges": top_edges_summary
    }

    # Save the final answer to a JSON file
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))