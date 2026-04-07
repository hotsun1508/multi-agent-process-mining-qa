import os
import pandas as pd
import pm4py
import json

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_df = pm4py.ocel_flattening(ocel, 'orders')
    
    # Step 2: Calculate sojourn time for each activity
    flattened_df = flattened_df.sort_values(by=['case:concept:name', 'time:timestamp'])
    flattened_df['next_timestamp'] = flattened_df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    flattened_df['sojourn_time'] = (flattened_df['next_timestamp'] - flattened_df['time:timestamp']).dt.total_seconds()
    
    # Step 3: Compute the mean sojourn time per activity
    mean_sojourn_time = flattened_df.groupby('concept:name')['sojourn_time'].mean().reset_index()
    mean_sojourn_time.columns = ['activity', 'mean_sojourn_time']
    
    # Step 4: Identify the activity with the largest mean sojourn time
    max_activity = mean_sojourn_time.loc[mean_sojourn_time['mean_sojourn_time'].idxmax()]
    
    # Step 5: Prepare the final result dictionary
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {
            'performance': {
                'activity': max_activity['activity'],
                'mean_sojourn_time': max_activity['mean_sojourn_time']
            }
        }
    }
    
    # Step 6: Save the mean sojourn time results to a CSV file
    mean_sojourn_time.to_csv(os.path.join(output_dir, 'mean_sojourn_time_orders.csv'), index=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'mean_sojourn_time_orders.csv')})')
    
    # Step 7: Save the final result as a JSON file
    with open(os.path.join(output_dir, 'final_result.json'), 'w') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_result.json')})')
    
    print(json.dumps(result, ensure_ascii=False))