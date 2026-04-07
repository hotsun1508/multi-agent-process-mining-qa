import os
import json
import pm4py
import pandas as pd

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    start_activities = log_df.groupby('case:concept:name')['concept:name'].first().unique().tolist()
    end_activities = log_df.groupby('case:concept:name')['concept:name'].last().unique().tolist()

    result_dict = {
        'start_activity': start_activities,
        'end_activity': end_activities
    }

    # Save the results in JSON format
    output_path = 'output/result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result_dict, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    final_answer = {'start_activity': start_activities, 'end_activity': end_activities}
    print(json.dumps(final_answer, ensure_ascii=False))