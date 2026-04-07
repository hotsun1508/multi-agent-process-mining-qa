import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'items'
    flattened_log = pm4py.ocel_flattening(ocel, 'items')

    # Step 2: Calculate sojourn time for each activity
    flattened_log = flattened_log.sort_values(by=['case:concept:name', 'time:timestamp'])
    flattened_log['next_timestamp'] = flattened_log.groupby('case:concept:name')['time:timestamp'].shift(-1)
    flattened_log['sojourn_time'] = (flattened_log['next_timestamp'] - flattened_log['time:timestamp']).dt.total_seconds()

    # Step 3: Compute the mean sojourn time per activity
    sojourn_df = flattened_log.groupby('concept:name')['sojourn_time'].mean().reset_index()
    sojourn_df.columns = ['activity', 'mean_sojourn_time']

    # Step 4: Identify the bottleneck activity
    bottleneck_activity = sojourn_df.loc[sojourn_df['mean_sojourn_time'].idxmax()].to_dict()

    # Step 5: Save the sojourn time table as a CSV file
    sojourn_df.to_csv(os.path.join(output_dir, 'sojourn_items.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'sojourn_items.csv')}')

    # Step 6: Save the bottleneck activity as a JSON file
    with open(os.path.join(output_dir, 'bottleneck_activity_items.json'), 'w') as f:
        json.dump(bottleneck_activity, f)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'bottleneck_activity_items.json')}')

    # Step 7: Prepare the final result dictionary
    final_answer = {
        'performance': {
            'mean_sojourn_times': sojourn_df.set_index('activity')['mean_sojourn_time'].to_dict(),
            'bottleneck_activity': bottleneck_activity
        }
    }

    print(json.dumps(final_answer, ensure_ascii=False))