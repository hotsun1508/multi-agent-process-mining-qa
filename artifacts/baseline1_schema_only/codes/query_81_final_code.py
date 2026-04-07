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
    case_durations = log_df.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    case_durations.columns = ['case:concept:name', 'duration']
    
    # Calculate average case duration
    average_duration = case_durations['duration'].mean()
    
    # Count frequency of each variant
    variant_counts = log_df['concept:name'].value_counts().reset_index()
    variant_counts.columns = ['variant', 'frequency']
    
    # Determine top 20% variants
    top_20_count = max(1, math.ceil(len(variant_counts) * 0.2))
    top_variants = variant_counts.nlargest(top_20_count, 'frequency')['variant']
    
    # Filter cases that are in the top variants
    top_cases = log_df[log_df['concept:name'].isin(top_variants)]
    
    # Calculate the percentage of cases in top variants with duration exceeding average duration
    top_case_durations = top_cases.groupby('case:concept:name')['time:timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).reset_index()
    top_case_durations.columns = ['case:concept:name', 'duration']
    delayed_cases_count = (top_case_durations['duration'] > average_duration).sum()
    percentage_delayed = (delayed_cases_count / len(top_case_durations)) * 100 if len(top_case_durations) > 0 else 0
    
    # Save the result to CSV
    result_df = pd.DataFrame({'percentage_delayed': [percentage_delayed]})
    result_df.to_csv('output/delayed_in_top20_orders.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/delayed_in_top20_orders.csv')
    
    # Prepare final answer
    final_answer = {'percentage_delayed': percentage_delayed}
    print(json.dumps(final_answer, ensure_ascii=False))