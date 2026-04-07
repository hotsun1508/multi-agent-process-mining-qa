import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Discover Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(['case:concept:name', 'concept:name'])['time:timestamp'].diff().dt.total_seconds().dropna()
    avg_durations = transition_durations.groupby(['case:concept:name']).mean().reset_index()
    avg_durations.columns = ['edge', 'avg_duration']
    slowest_edge = avg_durations.loc[avg_durations['avg_duration'].idxmax()]

    # Identify the slowest edge
    slowest_edge_name = slowest_edge['edge']
    slowest_duration = slowest_edge['avg_duration']

    # Find top 5 resources involved in the activities of the slowest edge
    involved_activities = slowest_edge_name.split(' -> ')
    resources = log_df[log_df['concept:name'].isin(involved_activities)]['org:resource']
    top_resources = resources.value_counts().head(5).index.tolist()

    # Determine the dominant variant among cases involving those resources and that edge
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    variants = pm4py.get_variants(filtered_cases)
    dominant_variant = max(variants.items(), key=lambda x: len(x[1]))[0]

    # Token-based replay conformance checking
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    fitting_cases = pm4py.conformance_token_based_replay(event_log, petri_net, initial_marking, final_marking)
    fitting_case_count = len(fitting_cases)

    # Prepare final answer
    final_answer = {
        'slowest_edge': slowest_edge_name,
        'slowest_duration': slowest_duration,
        'top_resources': top_resources,
        'dominant_variant': dominant_variant,
        'fitting_case_count': fitting_case_count
    }

    # Save results
    with open(os.path.join(output_dir, 'benchmark_results.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'benchmark_results.json')}')

    print(json.dumps(final_answer, ensure_ascii=False))