import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Discover the Heuristics Net
    heuristics_net, initial_marking, final_marking = pm4py.discover_heuristics_net(event_log)
    # Save the Heuristics Net visualization
    heuristics_net_path = 'output/heuristics_net.png'
    pm4py.save_vis_heuristics_net(heuristics_net, heuristics_net_path)
    print(f'OUTPUT_FILE_LOCATION: {heuristics_net_path}')  
    
    # Calculate the Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    # Find the strongest dependency relation
    strongest_dependency = max(dfg.items(), key=lambda x: x[1])
    source, target = strongest_dependency[0]
    transition_duration = []
    
    # Calculate transition durations
    for case_id, group in log_df.groupby('case:concept:name'):
        timestamps = group['time:timestamp'].tolist()
        activities = group['concept:name'].tolist()
        for i in range(len(activities) - 1):
            if activities[i] == source and activities[i + 1] == target:
                duration = (timestamps[i + 1] - timestamps[i]).total_seconds()
                transition_duration.append(duration)
    
    average_duration = sum(transition_duration) / len(transition_duration) if transition_duration else 0
    
    # Prepare the final answer
    final_answer = {
        'strongest_dependency': {'source': source, 'target': target},
        'average_transition_duration': average_duration
    }
    
    # Save the final answer to a JSON file
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()