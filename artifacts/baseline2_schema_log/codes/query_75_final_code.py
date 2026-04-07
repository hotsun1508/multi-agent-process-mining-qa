import pm4py
import pandas as pd
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for items
    flat_items = pm4py.ocel_flattening(ocel, object_type='items')
    # Calculate sojourn time for each activity
    flat_items['time:timestamp'] = pd.to_datetime(flat_items['time:timestamp'])
    flat_items = flat_items.sort_values(['case:concept:name', 'time:timestamp'])
    flat_items['sojourn_time'] = flat_items.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds().fillna(0)
    sojourn_table = flat_items.groupby('concept:name')['sojourn_time'].mean().reset_index()
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
    final_answer = {'bottleneck_activity': bottleneck_activity_dict['activity'], 'mean_sojourn_time': bottleneck_activity_dict['mean_sojourn_time']}
    print(json.dumps(final_answer, ensure_ascii=False))