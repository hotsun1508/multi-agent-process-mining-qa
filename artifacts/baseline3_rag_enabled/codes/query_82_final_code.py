import os
import pandas as pd
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'customers'
    flattened_log = pm4py.ocel_flattening(ocel, 'customers')

    # Step 2: Calculate the average case duration for the full flattened view
    full_case_durations = flattened_log.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    full_case_durations['duration'] = (full_case_durations['time:timestamp']['max'] - full_case_durations['time:timestamp']['min']).dt.total_seconds()
    overall_avg_duration = full_case_durations['duration'].mean()

    # Step 3: Identify the most dominant variant
    variant_counts = flattened_log['concept:name'].value_counts()
    most_dominant_variant = variant_counts.idxmax()

    # Step 4: Filter cases following the most dominant variant
    dominant_variant_cases = flattened_log[flattened_log['concept:name'] == most_dominant_variant]
    dominant_case_durations = dominant_variant_cases.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    dominant_case_durations['duration'] = (dominant_case_durations['time:timestamp']['max'] - dominant_case_durations['time:timestamp']['min']).dt.total_seconds()
    avg_dominant_duration = dominant_case_durations['duration'].mean()

    # Step 5: Calculate the difference in average case duration
    duration_delta = avg_dominant_duration - overall_avg_duration

    # Step 6: Save the result as most_dominant_variant_duration_delta.json
    result = {
        'dominant_variant': most_dominant_variant,
        'average_case_duration_delta': duration_delta
    }
    with open('output/most_dominant_variant_duration_delta.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/most_dominant_variant_duration_delta.json')

    # Step 7: Prepare the final benchmark answer
    final_answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {'dominant_variant': most_dominant_variant, 'average_case_duration_delta': duration_delta},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)']
    }
    print(json.dumps(final_answer, ensure_ascii=False))