import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for packages
    flat_log = pm4py.ocel_flattening(ocel, 'packages')
    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(flat_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate sojourn time for each activity
    log_df['duration'] = log_df.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds()
    mean_sojourn_time = log_df.groupby('concept:name')['duration'].mean().reset_index()
    mean_sojourn_time.columns = ['activity', 'mean_sojourn_time']
    
    # Save the mean sojourn time to CSV
    mean_sojourn_time.to_csv('output/activity_duration_packages.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/activity_duration_packages.csv')
    
    # Get top 5 activities with largest mean sojourn time
    top_5_activities = mean_sojourn_time.nlargest(5, 'mean_sojourn_time')
    top_5_activities.to_csv('output/top5_longest_activities_packages.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/top5_longest_activities_packages.csv')
    
    # Prepare final answer
    final_answer = {
        'mean_sojourn_time': mean_sojourn_time.to_dict(orient='records'),
        'top_5_activities': top_5_activities.to_dict(orient='records')
    }
    print(json.dumps(final_answer, ensure_ascii=False))