import os
import json
import pandas as pd
import pm4py


def main():
    event_log = ACTIVE_LOG
    os.makedirs('output', exist_ok=True)

    # Step 1: Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 2: Get the frequency of each variant
    variant_counts = log_df.groupby(['case:concept:name', 'concept:name']).size().reset_index(name='counts')
    top_variants = variant_counts.groupby('case:concept:name')['counts'].sum().nlargest(int(len(variant_counts) * 0.2)).index.tolist()

    # Step 3: Filter the DataFrame for the top 20% variants
    filtered_df = log_df[log_df['case:concept:name'].isin(top_variants)]

    # Step 4: Discover the Petri net model using the Inductive Miner algorithm
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_df)

    # Step 5: Perform token-based replay conformance checking
    replay_results = pm4py.conformance_diagnostics_token_based_replay(filtered_df, petri_net, initial_marking, final_marking)

    # Step 6: Calculate average throughput time for non-fit cases
    non_fit_cases = [result for result in replay_results if not result['trace_is_fit']]
    throughput_times = []
    for case in non_fit_cases:
        case_id = case['case_id']
        case_events = log_df[log_df['case:concept:name'] == case_id]
        if not case_events.empty:
            start_time = case_events['time:timestamp'].min()
            end_time = case_events['time:timestamp'].max()
            throughput_time = (end_time - start_time).total_seconds() / 3600  # Convert to hours
            throughput_times.append(throughput_time)
    average_throughput_time = sum(throughput_times) / len(throughput_times) if throughput_times else 0

    # Step 7: Save the Petri net model as a .png file
    petri_net_file_path = 'output/petri_net_model.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_file_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_file_path}')  

    # Step 8: Prepare the final benchmark answer as a JSON-serializable dictionary
    answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'petri_net',
            'conformance': 'conformance_summary',
            'performance': 'average_throughput_time'
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/json)'],
        'average_throughput_time': average_throughput_time
    }
    
    # Step 9: Save the final answer to a JSON file
    with open('output/benchmark_results.json', 'w') as f:
        json.dump(answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_results.json')
    
    print(json.dumps(answer, ensure_ascii=False))