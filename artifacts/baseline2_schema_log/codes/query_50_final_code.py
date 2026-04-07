import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Get the top 20% most frequent variants
    variant_counts = log_df['case:concept:name'].value_counts()
    top_variants = variant_counts.head(int(len(variant_counts) * 0.2)).index.tolist()
    filtered_log = log_df[log_df['case:concept:name'].isin(top_variants)]

    # Discover the Petri net from the reference model
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)

    # Token-based replay to find non-fit cases
    replay_result = pm4py.algo.conformance.token_based_replay.apply(filtered_log, petri_net, initial_marking, final_marking)
    non_fit_cases = [case['case_id'] for case in replay_result if case['fit'] == False]

    # Calculate sojourn times for non-fit cases
    non_fit_df = log_df[log_df['case:concept:name'].isin(non_fit_cases)]
    non_fit_df['duration'] = non_fit_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min())
    longest_activity = non_fit_df.groupby('concept:name')['duration'].mean().idxmax()

    # Get top 3 resources for the longest activity
    top_resources = non_fit_df[non_fit_df['concept:name'] == longest_activity]['org:resource'].value_counts().head(3)

    # Prepare final answer
    final_answer = {
        'longest_activity': longest_activity,
        'top_resources': top_resources.index.tolist(),
    }

    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()