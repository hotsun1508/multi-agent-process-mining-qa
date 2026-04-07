import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Step 1: Identify the top 5 resources by event frequency
    top_resources = log_df['org:resource'].value_counts().nlargest(5).index.tolist()
    
    # Step 2: Select cases involving at least one of the top resources
    filtered_cases = log_df[log_df['org:resource'].isin(top_resources)]
    
    # Step 3: Determine the dominant variant in that subset
    variants = pm4py.get_variants(filtered_cases)
    dominant_variant = max(variants.items(), key=lambda x: x[1])[0]
    
    # Step 4: Discover a Petri net from the cases of that variant
    variant_cases = filtered_cases[filtered_cases['case:concept:name'].isin(filtered_cases['case:concept:name'][filtered_cases['concept:name'].isin(dominant_variant)])]
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(variant_cases)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    
    # Step 5: Identify non-fit cases under token-based replay
    non_fit_cases = pm4py.conformance_token_based_replay(event_log, petri_net, initial_marking, final_marking)
    non_fit_cases_df = pd.DataFrame(non_fit_cases)
    average_throughput_time = non_fit_cases_df['time:timestamp'].mean() if not non_fit_cases_df.empty else 0
    
    # Final answer
    final_answer = {
        'resource': top_resources,
        'behavior_variant': dominant_variant,
        'process_discovery': 'output/petri_net.png',
        'conformance': average_throughput_time,
        'performance': len(non_fit_cases_df)
    }
    
    with open('output/final_benchmark.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_benchmark.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))