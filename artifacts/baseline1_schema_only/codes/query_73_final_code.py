import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Convert to DataFrame for easier manipulation
    log_df = pm4py.convert_to_dataframe(flattened_orders)
    # Sort values by case and timestamp
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    # Calculate sojourn time for each activity
    log_df['time:duration'] = log_df.groupby('case:concept:name')['time:timestamp'].diff().fillna(pd.Timedelta(0))
    # Calculate mean sojourn time per activity
    mean_sojourn_time = log_df.groupby('concept:name')['time:duration'].mean().dt.total_seconds()
    # Find the activity with the largest mean sojourn time
    max_activity = mean_sojourn_time.idxmax()
    max_duration = mean_sojourn_time.max()
    final_answer = {'activity': max_activity, 'mean_sojourn_time': max_duration}
    # Save the final answer to a JSON file
    output_path = 'output/mean_sojourn_time.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    print(json.dumps(final_answer, ensure_ascii=False))