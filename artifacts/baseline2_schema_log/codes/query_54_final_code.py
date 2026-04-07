import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time for each case
    throughput_times = log_df.groupby('case:concept:name').agg(
        throughput_time=('time:timestamp', lambda x: (x.max() - x.min()).total_seconds())
    ).reset_index()

    # Identify the slowest 10% of cases
    threshold = throughput_times['throughput_time'].quantile(0.9)
    slow_cases = throughput_times[throughput_times['throughput_time'] > threshold]

    # Determine the dominant variant among slow cases
    slow_case_ids = slow_cases['case:concept:name'].tolist()
    slow_log_df = log_df[log_df['case:concept:name'].isin(slow_case_ids)]
    variant_counts = slow_log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variant_counts.idxmax()

    # Discover a reference Petri net from the cases of that variant
    variant_cases = slow_log_df[slow_log_df['case:concept:name'].isin(slow_case_ids) & (slow_log_df['concept:name'].apply(lambda x: ' -> '.join(slow_log_df[slow_log_df['case:concept:name'] == x]['concept:name'].tolist()) == dominant_variant))]
    flat_variant_log = pm4py.convert_to_dataframe(variant_cases)
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flat_variant_log)
    pm4py.save_vis_petri_net(petri_net, 'output/petri_net.png')
    print('OUTPUT_FILE_LOCATION: output/petri_net.png')
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')

    # Identify cases that are not fit under token-based replay
    non_fit_cases = []
    for case_id in slow_case_ids:
        case_log = variant_cases[variant_cases['case:concept:name'] == case_id]
        if not pm4py.check_fit_token_based(case_log, petri_net, initial_marking):
            non_fit_cases.append(case_id)

    # Report the top 3 resources appearing in those non-fit cases
    non_fit_log_df = log_df[log_df['case:concept:name'].isin(non_fit_cases)]
    top_resources = non_fit_log_df['org:resource'].value_counts().head(3).to_dict()

    # Prepare final answer
    final_answer = {
        'slow_cases': slow_cases.to_dict(orient='records'),
        'dominant_variant': dominant_variant,
        'top_resources': top_resources
    }
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()