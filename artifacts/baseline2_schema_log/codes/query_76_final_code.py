import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for packages
    flattened_packages = pm4py.ocel_flattening(ocel, object_type='packages')
    # Convert to DataFrame
    df = pd.DataFrame(flattened_packages)
    # Ensure timestamp is in datetime format
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    # Calculate sojourn time for each activity
    df['sojourn_time'] = df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    # Calculate mean sojourn time per activity
    activity_duration = df.groupby('concept:name')['sojourn_time'].mean().reset_index()
    activity_duration.columns = ['activity', 'mean_sojourn_time']
    # Save the activity duration to CSV
    activity_duration.to_csv('output/activity_duration_packages.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/activity_duration_packages.csv')
    # Get top 5 activities with largest mean sojourn time
    top_5_activities = activity_duration.nlargest(5, 'mean_sojourn_time')
    top_5_activities.to_csv('output/top5_longest_activities_packages.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/top5_longest_activities_packages.csv')
    # Prepare final answer
    final_answer = {'mean_sojourn_time_per_activity': activity_duration.to_dict(orient='records'), 'top_5_activities': top_5_activities.to_dict(orient='records')}
    print(json.dumps(final_answer, ensure_ascii=False))