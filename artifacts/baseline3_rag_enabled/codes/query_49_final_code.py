import os
import json
import pandas as pd
import pm4py
from pm4py.algo.conformance.token_replay import algorithm as token_replay
from collections import Counter


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    os.makedirs('output', exist_ok=True)

    # Step 1: Calculate throughput time for each case
    case_times = log_df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_times['throughput_time'] = (case_times['max'] - case_times['min']).dt.total_seconds()

    # Step 2: Identify the slowest 10% of cases
    threshold_index = int(len(case_times) * 0.1)
    slowest_cases = case_times.nlargest(threshold_index, 'throughput_time')
    slowest_case_ids = slowest_cases.index.tolist()
    slowest_cases_df = log_df[log_df['case:concept:name'].isin(slowest_case_ids)]

    # Step 3: Load the reference Petri net
    petri_net, initial_marking, final_marking = pm4py.read_petri_net('path_to_petri_net')  # Adjust path accordingly

    # Step 4: Token-based replay to find non-fit cases
    non_fit_cases = token_replay.apply(event_log, petri_net, initial_marking, final_marking, variant=token_replay.Variants.TOKEN_BASED)
    non_fit_case_ids = [case['case:concept:name'] for case in non_fit_cases if case['fit'] == False]

    # Step 5: Filter non-fit cases from slowest cases
    non_fit_slowest_cases_df = slowest_cases_df[slowest_cases_df.index.isin(non_fit_case_ids)]

    # Step 6: Find the strongest Handover of Work pair
    handover_pairs = non_fit_slowest_cases_df.groupby(['concept:name']).size().reset_index(name='counts')
    strongest_handover = handover_pairs.nlargest(1, 'counts')
    handover_activity = strongest_handover['concept:name'].values[0]

    # Step 7: Find the dominant variant among cases containing that pair
    variants = non_fit_slowest_cases_df.groupby(['case:concept:name'])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    dominant_variant = variants.idxmax()

    # Step 8: Prepare final answer
    final_answer = {
        'slowest_case_ids': slowest_case_ids,
        'non_fit_case_ids': non_fit_case_ids,
        'strongest_handover': handover_activity,
        'dominant_variant': dominant_variant
    }

    # Step 9: Save the final answer to a JSON file
    with open('output/final_answer.json', 'w') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=4)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))