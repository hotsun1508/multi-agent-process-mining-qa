import pm4py
import pandas as pd
import json
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery import petri_net as pn_discovery
from pm4py.algo.conformance import token_replay
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate throughput time for each case
    log_df['throughput_time'] = log_df.groupby('case:concept:name')['time:timestamp'].transform(lambda x: x.max() - x.min())
    slowest_cases_threshold = log_df['throughput_time'].quantile(0.9)
    slowest_cases = log_df[log_df['throughput_time'] >= slowest_cases_threshold]

    # Load the reference Petri net
    petri_net, initial_marking, final_marking = pn_discovery.apply(event_log)

    # Token-based replay to find non-fit cases
    replay_results = token_replay.apply(event_log, petri_net, initial_marking, final_marking)
    non_fit_cases = [case for case in replay_results if case['fit'] == False]

    # Find the strongest Handover of Work pair among non-fit cases
    handover_pairs = []
    for case in non_fit_cases:
        activities = case['trace']
        for i in range(len(activities) - 1):
            if 'W_' in activities[i] and 'W_' in activities[i + 1]:
                handover_pairs.append((activities[i], activities[i + 1]))

    handover_counts = Counter(handover_pairs)
    strongest_handover = handover_counts.most_common(1)[0] if handover_counts else None

    # Report the dominant variant among cases containing that pair
    dominant_variant = None
    if strongest_handover:
        handover_activity_1, handover_activity_2 = strongest_handover[0]
        dominant_cases = slowest_cases[slowest_cases['concept:name'].str.contains(handover_activity_1) | slowest_cases['concept:name'].str.contains(handover_activity_2)]
        dominant_variant = dominant_cases['case:concept:name'].value_counts().idxmax() if not dominant_cases.empty else None

    # Prepare final answer
    final_answer = {
        'slowest_cases_count': len(slowest_cases),
        'non_fit_cases_count': len(non_fit_cases),
        'strongest_handover': strongest_handover,
        'dominant_variant': dominant_variant
    }

    # Save final answer to JSON
    output_path = 'output/final_answer.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))