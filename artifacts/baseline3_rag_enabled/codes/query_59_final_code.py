import os
import json
import pandas as pd
import pm4py
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.token_replay import algorithm as token_replay


def main():
    event_log = ACTIVE_LOG
    os.makedirs('output', exist_ok=True)

    # Step 1: Convert the event log to a DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Step 2: Discover the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = dfg_discovery.apply(event_log)

    # Step 3: Identify the most frequent edge
    most_frequent_edge = max(dfg.items(), key=lambda x: x[1])
    edge_source, edge_target = most_frequent_edge[0]

    # Step 4: Filter cases containing the most frequent edge
    filtered_cases = log_df[(log_df['concept:name'] == edge_source) | (log_df['concept:name'] == edge_target)]
    filtered_case_ids = filtered_cases['case:concept:name'].unique()
    filtered_df = log_df[log_df['case:concept:name'].isin(filtered_case_ids)]

    # Step 5: Determine the dominant variant within the filtered subset
    variants = pm4py.get_variants(filtered_df)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]

    # Step 6: Load the reference Petri net
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_df)

    # Step 7: Token-based replay to identify non-fit cases
    non_fit_cases = token_replay.apply(filtered_df, petri_net, initial_marking, final_marking, variant=token_replay.Variants.TOKEN_BASED)
    non_fit_case_ids = [case['case:concept:name'] for case in non_fit_cases if case['fit'] == False]
    non_fit_df = filtered_df[filtered_df['case:concept:name'].isin(non_fit_case_ids)]

    # Step 8: Calculate average throughput time of non-fit cases
    non_fit_df['time:timestamp'] = pd.to_datetime(non_fit_df['time:timestamp'])
    throughput_times = non_fit_df.groupby('case:concept:name').agg({'time:timestamp': ['min', 'max']})
    throughput_times['throughput_time'] = (throughput_times[('time:timestamp', 'max')] - throughput_times[('time:timestamp', 'min')]).dt.total_seconds() / 3600
    average_throughput_time = throughput_times['throughput_time'].mean()

    # Step 9: Identify top 3 resources in non-fit cases
    top_resources = non_fit_df['org:resource'].value_counts().head(3).to_dict()

    # Step 10: Prepare final answer
    final_answer = {
        'most_frequent_edge': {'source': edge_source, 'target': edge_target, 'count': most_frequent_edge[1]},
        'dominant_variant': dominant_variant,
        'average_throughput_time': average_throughput_time,
        'top_resources': top_resources
    }

    # Step 11: Save the final answer to a JSON file
    output_path = 'output/final_benchmark_answer.json'
    with open(output_path, 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=4)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))