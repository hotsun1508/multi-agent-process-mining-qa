import os
import pandas as pd
import json
import pm4py
import statistics

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_log = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Calculate case durations and isolate delayed cases
    case_durations = flattened_log.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    average_duration = statistics.mean(case_durations['duration'])
    delayed_cases = case_durations[case_durations['duration'] > average_duration].index.tolist()

    # Step 3: Filter the flattened log for delayed cases
    delayed_log = flattened_log[flattened_log['case:concept:name'].isin(delayed_cases)]

    # Step 4: Identify the most dominant variant within the delayed-case subset
    variant_counts = delayed_log['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()

    # Step 5: Filter the delayed log for the dominant variant
    dominant_variant_log = delayed_log[delayed_log['concept:name'] == dominant_variant]

    # Step 6: Discover the DFG on the cases that are both delayed and belong to the dominant variant
    dfg, start_activities, end_activities = pm4py.discover_dfg(dominant_variant_log)

    # Step 7: Save DFG to CSV and PNG
    dfg_df = pd.DataFrame.from_dict(dfg, orient='index', columns=['count']).reset_index()
    dfg_df.columns = ['source', 'target', 'count']
    dfg_df.to_csv(os.path.join(output_dir, 'dfg_dom_delayed.csv'), index=False)
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, os.path.join(output_dir, 'dfg_dom_delayed.png'))

    # Step 8: Prepare final answer
    final_answer = {
        'dominant_variant': dominant_variant,
        'average_case_duration': average_duration,
        'dfg': dfg_df.to_dict(orient='records')
    }

    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'dfg_dom_delayed.csv')}')
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'dfg_dom_delayed.png')}')
    print(json.dumps(final_answer, ensure_ascii=False))