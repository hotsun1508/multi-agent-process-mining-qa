import os
import json
import pm4py
import pandas as pd


def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Calculate Directly-Follows Graph (DFG)
    dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

    # Calculate average transition durations
    transition_durations = {}
    for case_id, group in log_df.groupby('case:concept:name'):
        timestamps = group['time:timestamp'].astype('datetime64[ns]').tolist()
        activities = group['concept:name'].tolist()
        for i in range(len(activities) - 1):
            edge = (activities[i], activities[i + 1])
            duration = (timestamps[i + 1] - timestamps[i]).total_seconds()
            if edge not in transition_durations:
                transition_durations[edge] = []
            transition_durations[edge].append(duration)

    # Find the edge with the highest average duration
    highest_avg_duration_edge = None
    highest_avg_duration = 0
    for edge, durations in transition_durations.items():
        avg_duration = sum(durations) / len(durations)
        if avg_duration > highest_avg_duration:
            highest_avg_duration = avg_duration
            highest_avg_duration_edge = edge

    # Calculate the percentage of cases containing that edge in the top 20% variants
    top_variants = log_df['case:concept:name'].value_counts().nlargest(int(len(log_df['case:concept:name'].unique()) * 0.2)).index.tolist()
    cases_with_edge = log_df[(log_df['concept:name'].shift() == highest_avg_duration_edge[0]) & (log_df['concept:name'] == highest_avg_duration_edge[1])]
    top_variant_cases = cases_with_edge[cases_with_edge['case:concept:name'].isin(top_variants)]
    percentage_top_variants = (len(top_variant_cases) / len(cases_with_edge) * 100) if len(cases_with_edge) > 0 else 0

    # Prepare final answer
    final_answer = {
        'dfg': dfg,
        'highest_avg_duration_edge': {
            'edge': highest_avg_duration_edge,
            'avg_duration': highest_avg_duration,
            'percentage_in_top_variants': percentage_top_variants
        }
    }

    # Save DFG visualization
    dfg_visualization_path = os.path.join(output_dir, 'dfg_visualization.png')
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, dfg_visualization_path)
    print(f'OUTPUT_FILE_LOCATION: {dfg_visualization_path}')  

    # Save final answer to JSON
    with open(os.path.join(output_dir, 'final_answer.json'), 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(json.dumps(final_answer, ensure_ascii=False))