import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min())
    slowest_cases = log_df.groupby('case:concept:name')['throughput_time'].first().nsmallest(int(len(log_df['case:concept:name'].unique()) * 0.1)).index.tolist()
    slowest_log_df = log_df[log_df['case:concept:name'].isin(slowest_cases)]
    slowest_log = pm4py.convert_to_event_log(slowest_log_df)

    # Load the reference Petri net
    reference_net, initial_marking, final_marking = pm4py.read_pnml('/path/to/reference_petri_net.pnml')

    # Token-based replay to find non-fit cases
    replay_results = pm4py.algo.conformance.token_replay.apply(slowest_log, reference_net, initial_marking, final_marking)
    non_fit_cases = [case for case, result in replay_results.items() if not result['fit']]

    # Find the strongest Handover of Work pair among non-fit cases
    handover_pairs = []
    for case in non_fit_cases:
        case_events = slowest_log_df[slowest_log_df['case:concept:name'] == case]
        handover_pairs.extend(case_events['concept:name'].value_counts().items())
    handover_pairs_count = pd.Series(dict(handover_pairs)).value_counts()
    strongest_handover = handover_pairs_count.idxmax()

    # Report the dominant variant among cases containing that pair
    dominant_variant = slowest_log_df[slowest_log_df['concept:name'].str.contains(str(strongest_handover))]['case:concept:name'].value_counts().idxmax()

    # Prepare final answer
    final_answer = {
        'slowest_cases': len(slowest_cases),
        'non_fit_cases': len(non_fit_cases),
        'strongest_handover': strongest_handover,
        'dominant_variant': dominant_variant
    }

    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    print(json.dumps(final_answer, ensure_ascii=False))