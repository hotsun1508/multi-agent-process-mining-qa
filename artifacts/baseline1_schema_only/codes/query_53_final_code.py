import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(["concept:name", "case:concept:name"])['time:timestamp'].apply(lambda x: x.max() - x.min()).reset_index()
    transition_durations['duration'] = transition_durations['time:timestamp'].dt.total_seconds()
    avg_durations = transition_durations.groupby(["concept:name"])['duration'].mean().reset_index()
    slowest_edge = avg_durations.loc[avg_durations['duration'].idxmax()]
    slowest_activity = slowest_edge['concept:name']
    slowest_duration = slowest_edge['duration']
    
    # Identify top 5 resources involved in the slowest edge's activities
    involved_resources = log_df[log_df['concept:name'] == slowest_activity]['org:resource'].value_counts().head(5).index.tolist()
    
    # Determine dominant variant among cases involving those resources and the slowest edge
    filtered_cases = log_df[log_df['org:resource'].isin(involved_resources)]
    dominant_variant = filtered_cases.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='counts').sort_values(by='counts', ascending=False).head(1)
    dominant_variant_name = dominant_variant.iloc[0]['concept:name']
    
    # Check how many cases fit under token-based replay with the given reference Petri net
    petri_net = pm4py.discover_petri_net_inductive(event_log)
    token_based_replay = pm4py.replay_fitness(event_log, petri_net)
    fit_cases_count = sum(1 for case in token_based_replay if case['fit'])
    
    # Prepare final answer
    final_answer = {
        "slowest_edge": slowest_activity,
        "slowest_duration": slowest_duration,
        "top_resources": involved_resources,
        "dominant_variant": dominant_variant_name,
        "fit_cases_count": fit_cases_count
    }
    
    # Save outputs
    os.makedirs('output', exist_ok=True)
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))