import pm4py
import pandas as pd
import numpy as np
import json
import os
import statistics

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 2: Get the top 20% most frequent variants
    variant_counts = df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_df = df[df['case:concept:name'].isin(top_variants)]
    
    # Step 3: Discover the Petri net model using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_df)
    
    # Step 4: Perform token-based replay conformance checking
    replay_results = pm4py.conformance_diagnostics_token_based_replay(filtered_df, petri_net, initial_marking, final_marking)
    
    # Step 5: Identify non-fit cases
    non_fit_cases = [result for result in replay_results if not result['trace_is_fit']]
    
    # Step 6: Determine bottleneck activity with longest average sojourn time
    activity_times = {}  # Dictionary to hold activity times
    for case in non_fit_cases:
        case_id = case['case_id']
        case_events = df[df['case:concept:name'] == case_id]
        for activity in case_events['concept:name']:
            duration = (case_events['time:timestamp'].max() - case_events['time:timestamp'].min()).total_seconds()
            if activity not in activity_times:
                activity_times[activity] = []
            activity_times[activity].append(duration)
    
    # Calculate average sojourn time for each activity
    average_times = {activity: np.mean(times) for activity, times in activity_times.items()}
    bottleneck_activity = max(average_times, key=average_times.get)
    
    # Step 7: Report top 3 resources executing the bottleneck activity
    resources = df[df['concept:name'] == bottleneck_activity]['org:resource'].value_counts().head(3)
    top_resources = resources.index.tolist()
    
    # Step 8: Identify the most frequent variant among cases involving at least one of those resources
    relevant_cases = df[df['org:resource'].isin(top_resources)]
    relevant_variants = relevant_cases.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variant = relevant_variants.idxmax() if not relevant_variants.empty else None
    
    # Step 9: Prepare the final benchmark answer as a JSON-serializable dictionary
    answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'composite',
        'view': 'event_log',
        'result_schema': {
            'process_discovery': 'petri_net',
            'conformance': 'non_fit_cases',
            'performance': 'bottleneck_activity',
            'resource': 'top_resources',
            'behavior_variant': 'most_frequent_variant'
        },
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'bottleneck_activity': bottleneck_activity,
        'top_resources': top_resources,
        'most_frequent_variant': most_frequent_variant
    }
    
    # Step 10: Save the Petri net model as a .pkl file
    petri_net_filename = 'output/petri_net_model.pkl'
    pm4py.save_vis_petri_net(petri_net, petri_net_filename)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_filename}')  
    
    # Step 11: Save the final answer to a JSON file
    answer_filename = 'output/benchmark_answer.json'
    with open(answer_filename, 'w') as f:
        json.dump(answer, f, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: {answer_filename}')  
    
    print(json.dumps(answer, ensure_ascii=False))