import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flattened_orders = pm4py.ocel_flattening(ocel, object_type='orders')
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(flattened_orders)
    df = df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate sojourn time for each activity
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    df['sojourn_time'] = df.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds().fillna(0)
    
    # Calculate mean sojourn time per activity
    mean_sojourn_time = df.groupby('concept:name')['sojourn_time'].mean().reset_index()
    
    # Find the activity with the largest mean sojourn time
    max_activity = mean_sojourn_time.loc[mean_sojourn_time['sojourn_time'].idxmax()]
    
    # Prepare the final answer
    final_answer = {
        'activity': max_activity['concept:name'],
        'mean_sojourn_time': max_activity['sojourn_time']
    }
    
    # Save the final answer to a JSON file
    output_path = 'output/mean_sojourn_time.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))