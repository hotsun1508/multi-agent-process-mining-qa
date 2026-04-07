import pm4py
import json
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, object_type='customers')
    
    # Convert to DataFrame for analysis
    log_df = pm4py.convert_to_dataframe(flattened_customers)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Calculate case durations
    case_durations = log_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    case_durations['duration'] = case_durations['time:timestamp']['max'] - case_durations['time:timestamp']['min']
    case_durations = case_durations['duration'].dt.total_seconds()  # Convert to seconds
    
    # Calculate average case duration for all cases
    overall_avg_duration = case_durations.mean()
    
    # Identify the most dominant variant
    dominant_variant = case_durations.value_counts().idxmax()
    
    # Calculate average case duration for the dominant variant
    dominant_variant_duration = case_durations[case_durations == dominant_variant].mean()
    
    # Calculate the difference
    duration_difference = dominant_variant_duration - overall_avg_duration
    
    # Save the result
    result = {
        'dominant_variant_duration': dominant_variant_duration,
        'overall_avg_duration': overall_avg_duration,
        'duration_difference': duration_difference
    }
    with open('output/most_dominant_variant_duration_delta.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/most_dominant_variant_duration_delta.json')
    
    # Prepare final answer for benchmark
    final_answer = {
        'behavior_variant': str(dominant_variant),
        'performance': {
            'dominant_variant_duration': dominant_variant_duration,
            'overall_avg_duration': overall_avg_duration,
            'duration_difference': duration_difference
        }
    }
    print(json.dumps(final_answer, ensure_ascii=False))