import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)

    # Discover the Heuristics Net using default parameters
    heuristics_net = pm4py.discover_heuristics_net(event_log)

    # Save the discovered Heuristics Net model as a .pkl file
    model_path = os.path.join(output_dir, 'heuristics_net_model.pkl')
    with open(model_path, 'wb') as f:
        pm4py.save_pickle_output(heuristics_net, f)
    print(f'OUTPUT_FILE_LOCATION: {model_path}')  

    # Extract dependencies
    dependency_matrix = heuristics_net.dependency_matrix
    dependencies = [(source, target, weight) for source, targets in dependency_matrix.items() for target, weight in targets.items()]
    strongest_dependency = max(dependencies, key=lambda x: x[2])

    # Calculate the average duration for the transitions between the source and target activities
    source_activity = strongest_dependency[0]
    target_activity = strongest_dependency[1]
    weight = strongest_dependency[2]

    transition_durations = []
    for case_id, group in df.groupby('case:concept:name'):
        timestamps = group[group['concept:name'].isin([source_activity, target_activity])]['time:timestamp']
        if len(timestamps) >= 2:
            for i in range(len(timestamps) - 1):
                if group.iloc[i]['concept:name'] == source_activity and group.iloc[i + 1]['concept:name'] == target_activity:
                    transition_durations.append((timestamps.iloc[i + 1] - timestamps.iloc[i]).total_seconds())

    avg_transition_duration = sum(transition_durations) / len(transition_durations) if transition_durations else 0

    # Construct the final JSON-serializable dictionary
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {'process_discovery': 'heuristics_net'},
        'artifacts_schema': ['output/* (optional auxiliary artifacts such as png/csv/pkl/json)'],
        'strongest_dependency': {'source': source_activity, 'target': target_activity, 'weight': weight},
        'avg_transition_duration_seconds': avg_transition_duration
    }

    # Save the result to a JSON file
    result_path = os.path.join(output_dir, 'result.json')
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {result_path}')  

    print(json.dumps(result, ensure_ascii=False))