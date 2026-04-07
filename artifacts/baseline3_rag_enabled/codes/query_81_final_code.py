import os
import pandas as pd
import json
import pm4py
import math

def main():
    ocel = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_log = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Calculate case durations
    case_durations = flattened_log.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()
    average_duration = case_durations['duration'].mean()

    # Step 3: Count variants and their frequencies
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_count = max(1, math.ceil(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()

    # Step 4: Filter cases in the top variants
    top_variant_cases = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    top_variant_case_durations = top_variant_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    top_variant_case_durations['duration'] = (top_variant_case_durations['max'] - top_variant_case_durations['min']).dt.total_seconds()
    exceeding_cases_count = top_variant_case_durations[top_variant_case_durations['duration'] > average_duration].shape[0]
    total_top_variant_cases = top_variant_case_durations.shape[0]

    # Step 5: Calculate the percentage
    exceeding_percentage = (exceeding_cases_count / total_top_variant_cases * 100) if total_top_variant_cases > 0 else 0

    # Step 6: Save the result to CSV
    result_df = pd.DataFrame({'exceeding_cases_percentage': [exceeding_percentage]})
    result_csv_path = os.path.join(output_dir, 'delayed_in_top20_orders.csv')
    result_df.to_csv(result_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {result_csv_path}')  

    # Step 7: Prepare final answer
    final_answer = {
        'exceeding_cases_percentage': exceeding_percentage,
        'average_case_duration': average_duration
    }
    print(json.dumps(final_answer, ensure_ascii=False))