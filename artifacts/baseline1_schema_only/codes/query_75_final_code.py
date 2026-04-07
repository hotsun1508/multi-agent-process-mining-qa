import pandas as pd
import json
import os
import numpy as np


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for items
    flattened_items = pm4py.ocel_flattening(ocel, 'items')
    # Convert to DataFrame
    df = pm4py.convert_to_dataframe(flattened_items)
    df = df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate sojourn time for each activity
    df['next_timestamp'] = df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    df['sojourn_time'] = (df['next_timestamp'] - df['time:timestamp']).dt.total_seconds()
    sojourn_table = df.groupby('concept:name')['sojourn_time'].mean().reset_index()
    sojourn_table.columns = ['activity', 'mean_sojourn_time']
    
    # Identify the bottleneck activity
    bottleneck_activity = sojourn_table.loc[sojourn_table['mean_sojourn_time'].idxmax()]
    bottleneck_activity_dict = {'activity': bottleneck_activity['activity'], 'mean_sojourn_time': bottleneck_activity['mean_sojourn_time']}
    
    # Save sojourn table and bottleneck activity
    sojourn_table.to_csv('output/sojourn_items.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/sojourn_items.csv')
    
    with open('output/bottleneck_activity_items.json', 'w', encoding='utf-8') as f:
        json.dump(bottleneck_activity_dict, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/bottleneck_activity_items.json')
    
    # Prepare final answer
    final_answer = {'performance': {'bottleneck_activity': bottleneck_activity_dict['activity'], 'mean_sojourn_time': bottleneck_activity_dict['mean_sojourn_time']}}
    print(json.dumps(final_answer, ensure_ascii=False))