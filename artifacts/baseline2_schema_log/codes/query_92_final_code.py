def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Calculate case durations
    flat_orders['case_duration'] = flat_orders.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    # Calculate average case duration
    average_duration = flat_orders['case_duration'].mean()
    # Isolate delayed cases
    delayed_cases = flat_orders[flat_orders['case_duration'] > average_duration]
    # Identify the most dominant variant in the delayed cases
    variant_counts = delayed_cases['concept:name'].value_counts()
    dominant_variant = variant_counts.idxmax()
    # Filter delayed cases for the dominant variant
    dominant_delayed_cases = delayed_cases[delayed_cases['concept:name'] == dominant_variant]
    # Discover DFG on the dominant delayed cases
    dfg, start_activities, end_activities = pm4py.discover_dfg(dominant_delayed_cases)
    # Save DFG to CSV and PNG
    dfg_csv_path = 'output/dfg_dom_delayed.csv'
    dfg_png_path = 'output/dfg_dom_delayed.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_png_path)
    pd.DataFrame.from_dict(dfg, orient='index').to_csv(dfg_csv_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_csv_path}')  
    print(f'OUTPUT_FILE_LOCATION: {dfg_png_path}')  
    # Prepare final answer
    final_answer = {
        'behavior_variant': dominant_variant,
        'performance': average_duration,
        'process_discovery': dfg
    }
    print(json.dumps(final_answer, ensure_ascii=False))