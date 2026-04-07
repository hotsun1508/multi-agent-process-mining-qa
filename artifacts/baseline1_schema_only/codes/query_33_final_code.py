import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    variant_counts = log_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    top_50_percent_variants = variant_counts.head(len(variant_counts) // 2).index.tolist()
    filtered_cases = log_df[log_df.groupby('case:concept:name')['concept:name'].transform(lambda x: ' -> '.join(x) in top_50_percent_variants)]
    filtered_event_log = pm4py.convert_to_event_log(filtered_cases)
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_event_log)
    png_path = 'output/reference_petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    conformance_results = pm4py.conformance_token_based_replay(filtered_event_log, petri_net, initial_marking, final_marking)
    fit_cases_count = sum(1 for result in conformance_results if result['fit'])
    final_answer = {'top_50_percent_variants_count': len(top_50_percent_variants), 'fit_cases_count': fit_cases_count}
    with open('output/benchmark_results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_results.json')
    print(json.dumps(final_answer, ensure_ascii=False))