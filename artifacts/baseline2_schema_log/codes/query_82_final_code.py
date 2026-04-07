def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, object_type='customers')
    # Calculate case durations
    case_durations = flattened_customers.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    # Calculate average case duration for all cases
    overall_avg_duration = case_durations['duration'].mean()
    # Identify the most dominant variant
    variant_counts = flattened_customers['concept:name'].value_counts()
    most_dominant_variant = variant_counts.idxmax()
    # Filter cases for the most dominant variant
    dominant_variant_cases = flattened_customers[flattened_customers['concept:name'] == most_dominant_variant]
    # Calculate average case duration for the dominant variant
    dominant_variant_case_durations = dominant_variant_cases.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    dominant_variant_case_durations['duration'] = dominant_variant_case_durations['max'] - dominant_variant_case_durations['min']
    dominant_avg_duration = dominant_variant_case_durations['duration'].mean()
    # Calculate the difference
    duration_delta = dominant_avg_duration - overall_avg_duration
    # Save the result
    result = {'most_dominant_variant': most_dominant_variant, 'dominant_avg_duration': dominant_avg_duration.total_seconds(), 'overall_avg_duration': overall_avg_duration.total_seconds(), 'duration_delta': duration_delta.total_seconds()}
    with open('output/most_dominant_variant_duration_delta.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/most_dominant_variant_duration_delta.json')
    # Prepare final answer
    final_answer = {'behavior_variant': most_dominant_variant, 'performance': {'dominant_avg_duration': dominant_avg_duration.total_seconds(), 'overall_avg_duration': overall_avg_duration.total_seconds(), 'duration_delta': duration_delta.total_seconds()}}
    print(json.dumps(final_answer, ensure_ascii=False))