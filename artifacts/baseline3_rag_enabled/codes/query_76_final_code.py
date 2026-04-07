import os
import pandas as pd
import pm4py
import json
from pm4py.objects.ocel.obj import OCEL

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'packages'
    flattened_log = pm4py.ocel_flattening(ocel, 'packages')

    # Step 2: Calculate sojourn time for each activity
    flattened_log = flattened_log.sort_values(by=['case:concept:name', 'time:timestamp'])
    flattened_log['next_timestamp'] = flattened_log.groupby('case:concept:name')['time:timestamp'].shift(-1)
    flattened_log['sojourn_time'] = (flattened_log['next_timestamp'] - flattened_log['time:timestamp']).dt.total_seconds()

    # Step 3: Compute the mean sojourn time per activity
    mean_sojourn_time = flattened_log.groupby('concept:name')['sojourn_time'].mean().reset_index()
    mean_sojourn_time.columns = ['activity', 'mean_sojourn_time']

    # Step 4: Save the mean sojourn time results to activity_duration_packages.csv
    mean_sojourn_time.to_csv(os.path.join(output_dir, 'activity_duration_packages.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'activity_duration_packages.csv')}')

    # Step 5: Identify the top-5 activities with the largest mean sojourn time
    top5_activities = mean_sojourn_time.nlargest(5, 'mean_sojourn_time')
    top5_activities.to_csv(os.path.join(output_dir, 'top5_longest_activities_packages.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'top5_longest_activities_packages.csv')}')

    # Step 6: Prepare the final result dictionary
    result = {
        'performance': {
            'mean_sojourn_times': mean_sojourn_time.set_index('activity')['mean_sojourn_time'].to_dict(),
            'top_5_activities': top5_activities.to_dict(orient='records')
        }
    }

    print(json.dumps(result, ensure_ascii=False))