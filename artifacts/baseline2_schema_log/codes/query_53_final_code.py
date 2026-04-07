import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

    # Calculate transition durations
    log_df['duration'] = log_df['time:timestamp'].diff().dt.total_seconds().fillna(0)
    transition_durations = log_df.groupby(['concept:name']).agg({'duration': 'mean'}).reset_index()

    # Identify the slowest edge
    slowest_edge = transition_durations.loc[transition_durations['duration'].idxmax()]
    slowest_activity = slowest_edge['concept:name']
    slowest_duration = slowest_edge['duration']

    # Identify the edge activities
    edge_activities = [src for src, tgt in dfg.keys() if src == slowest_activity]
    involved_resources = log_df[log_df['concept:name'].isin(edge_activities)]['org:resource'].value_counts().head(5).index.tolist()

    # Determine dominant variant
    filtered_cases = log_df[log_df['org:resource'].isin(involved_resources)]
    case_variants = filtered_cases.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = case_variants.idxmax() if not case_variants.empty else None

    # Token-based replay
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    token_based_replay_cases = pm4py.replay_fitness(event_log, petri_net, initial_marking, final_marking)
    fit_cases_count = sum(1 for case in token_based_replay_cases if case['fitness'] == 'fit')

    # Save outputs
    output_dir = 'output/'
    os.makedirs(output_dir, exist_ok=True)
    final_answer = {
        'slowest_edge': slowest_activity,
        'slowest_duration': slowest_duration,
        'top_resources': involved_resources,
        'dominant_variant': dominant_variant,
        'fit_cases_count': fit_cases_count
    }
    with open(os.path.join(output_dir, 'final_benchmark.json'), 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'final_benchmark.json')}')
    print(json.dumps(final_answer, ensure_ascii=False))