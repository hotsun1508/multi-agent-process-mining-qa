import os
import json
import pandas as pd
import pm4py


def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Generate Performance Spectrum
    performance_spectrum_path = os.path.join(output_dir, 'performance_spectrum.png')
    pm4py.visualization.performance_spectrum.apply(event_log, output_path=performance_spectrum_path)
    print(f'OUTPUT_FILE_LOCATION: {performance_spectrum_path}')  

    # Calculate elapsed times between transitions
    transitions = []
    for case in event_log:
        case_events = sorted(case, key=lambda x: x['time:timestamp'])
        activities = [event['concept:name'] for event in case_events]
        timestamps = [event['time:timestamp'] for event in case_events]

        for i in range(len(activities) - 1):
            transition = (activities[i], activities[i + 1])
            elapsed_time = (timestamps[i + 1] - timestamps[i]).total_seconds()
            transitions.append((transition, elapsed_time))

    # Calculate median elapsed times for each transition
    transition_durations = pd.DataFrame(transitions, columns=['transition', 'elapsed_time'])
    median_durations = transition_durations.groupby('transition')['elapsed_time'].median().reset_index()
    top_transitions = median_durations.nlargest(3, 'elapsed_time')

    # Prepare final answer
    final_answer = {
        'top_transitions': [
            {'from': transition[0], 'to': transition[1], 'median_seconds': median, 'median_readable': str(pd.to_timedelta(median, unit='s')), 'count': transition_durations[transition_durations['transition'] == transition].shape[0]}
            for transition, median in zip(top_transitions['transition'], top_transitions['elapsed_time'])
        ]
    }

    # Save final answer to JSON
    result_json_path = os.path.join(output_dir, 'result.json')
    with open(result_json_path, 'w') as json_file:
        json.dump(final_answer, json_file, ensure_ascii=False)
    print(f'OUTPUT_FILE_LOCATION: {result_json_path}')  

    # Save final answer to CSV
    result_csv_path = os.path.join(output_dir, 'result.csv')
    top_transitions.to_csv(result_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {result_csv_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))