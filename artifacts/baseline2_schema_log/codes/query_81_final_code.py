import pm4py
import pandas as pd
import json
import os
import math

def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Convert to DataFrame for easier manipulation
    log_df = pm4py.convert_to_dataframe(flat_orders)
    
    # Calculate case durations
    case_durations = log_df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['duration'] = case_durations['max'] - case_durations['min']
    average_duration = case_durations['duration'].mean()
    
    # Count the frequency of each variant
    variant_counts = log_df['concept:name'].value_counts()
    top_20_percent_count = max(1, math.ceil(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    
    # Filter cases that are in the top variants
    top_cases = log_df[log_df['concept:name'].isin(top_variants)]
    top_case_durations = case_durations.loc[top_cases['case:concept:name']].reset_index()
    
    # Calculate the percentage of cases exceeding the average duration
    delayed_cases_count = (top_case_durations['duration'] > average_duration).sum()
    total_cases_count = len(top_case_durations)
    percentage_delayed = (delayed_cases_count / total_cases_count * 100) if total_cases_count > 0 else 0
    
    # Save the result to CSV
    result_df = pd.DataFrame({'percentage_delayed': [percentage_delayed]})
    result_csv_path = 'output/delayed_in_top20_orders.csv'
    result_df.to_csv(result_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {result_csv_path}')  
    
    # Prepare final answer
    final_answer = {
        'average_case_duration': average_duration,
        'top_20_percent_count': top_20_percent_count,
        'percentage_delayed': percentage_delayed
    }
    print(json.dumps(final_answer, ensure_ascii=False))