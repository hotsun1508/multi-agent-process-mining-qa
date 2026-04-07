import pm4py
import pandas as pd
import os
import json

def main():
    event_log = ACTIVE_LOG
    # Ensure the output directory exists
    os.makedirs("output", exist_ok=True)

    # Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate sojourn time for each event
    log_df['next_timestamp'] = log_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    log_df['sojourn_time'] = (log_df['next_timestamp'] - log_df['time:timestamp']).dt.total_seconds()

    # Calculate average sojourn time for each activity
    average_sojourn_times = log_df.groupby('concept:name')['sojourn_time'].mean().dropna()

    # Identify the activity with the longest average sojourn time
    longest_sojourn_activity = average_sojourn_times.idxmax()
    longest_sojourn_time = average_sojourn_times.max()

    # Filter the DataFrame for the identified activity
    activity_df = log_df[log_df['concept:name'] == longest_sojourn_activity]

    # Find the top 5 resources executing the identified activity most frequently
    top_resources = activity_df['org:resource'].value_counts().head(5).index.tolist()

    # Prepare the final result dictionary
    result = {
        "primary_answer_in_csv_log": True,
        "result_type": "composite",
        "view": "event_log",
        "result_schema": {
            "performance": {
                "activity": longest_sojourn_activity,
                "average_sojourn_time": longest_sojourn_time
            },
            "resource": top_resources
        },
        "artifacts_schema": ["output/*"]
    }

    # Save the result to a JSON file
    with open("output/result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/result.json")

    # Print the final answer
    print(json.dumps(result, ensure_ascii=False))