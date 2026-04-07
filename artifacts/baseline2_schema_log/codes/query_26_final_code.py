import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)
    transition_durations = log_df.groupby(['concept:name', 'case:concept:name'])['time:timestamp'].agg(['min', 'max']).reset_index()
    transition_durations['duration'] = (transition_durations['max'] - transition_durations['min']).dt.total_seconds()
    transition_durations = transition_durations.groupby(['concept:name']).agg({'duration': 'mean'}).reset_index()
    dfg_with_durations = {edge: (count, transition_durations.loc[transition_durations['concept:name'] == edge[1], 'duration'].values[0] if not transition_durations.loc[transition_durations['concept:name'] == edge[1], 'duration'].empty else 0) for edge, count in dfg.items()}
    highest_avg_duration_edge = max(dfg_with_durations.items(), key=lambda x: x[1][1])
    final_answer = {'highest_avg_duration_edge': highest_avg_duration_edge[0], 'average_duration': highest_avg_duration_edge[1][1]}
    png_path = 'output/dfg_visualization.png'
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  
    with open('output/dfg.pkl', 'wb') as f:
        pickle.dump(dfg, f)
    print('OUTPUT_FILE_LOCATION: output/dfg.pkl')
    print(json.dumps(final_answer, ensure_ascii=False))