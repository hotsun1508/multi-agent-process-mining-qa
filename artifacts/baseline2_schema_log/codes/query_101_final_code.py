import pm4py
import json
import pandas as pd


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_log = pm4py.ocel_flattening(ocel, 'orders')
    # Discover variants and their frequencies
    variant_counts = flat_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    # Filter the log for top 20% variants
    model_building_sublog = flat_log[flat_log['concept:name'].isin(top_variants)]
    # Discover Petri net from the model-building sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(model_building_sublog)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Perform token-based replay
    replay_results = pm4py.replay_log(model_building_sublog, petri_net, initial_marking, final_marking)
    # Isolate non-fit cases
    non_fit_cases = [case for case in replay_results if not case['fit']]
    # Calculate average case duration
    case_durations = model_building_sublog.groupby('case:concept:name')['time:timestamp'].apply(lambda x: (x.max() - x.min()).total_seconds()).tolist()
    average_duration = sum(case_durations) / len(case_durations) if case_durations else 0
    # Calculate percentage of non-fit cases exceeding average duration
    non_fit_durations = [duration for case in non_fit_cases for duration in case_durations if case['case_id'] == duration['case:concept:name']]
    non_fit_exceeding_avg = sum(1 for duration in non_fit_durations if duration > average_duration)
    percentage_non_fit_exceeding_avg = (non_fit_exceeding_avg / len(non_fit_cases) * 100) if non_fit_cases else 0
    # Save results
    final_answer = {
        'percentage_non_fit_exceeding_avg': percentage_non_fit_exceeding_avg,
        'total_non_fit_cases': len(non_fit_cases),
        'average_case_duration': average_duration
    }
    with open('output/results.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/results.json')
    print(json.dumps(final_answer, ensure_ascii=False))