import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Generate Performance Spectrum visualization
    performance_spectrum_path = 'output/performance_spectrum.png'
    pm4py.visualization.performance_spectrum.apply(log_df, output_path=performance_spectrum_path)
    print(f'OUTPUT_FILE_LOCATION: {performance_spectrum_path}')  
    
    # Calculate elapsed times between activity transitions
    log_df['next_activity'] = log_df.groupby('case:concept:name')['concept:name'].shift(-1)
    log_df['elapsed_time'] = (log_df['time:timestamp'].shift(-1) - log_df['time:timestamp']).dt.total_seconds()
    transitions = log_df[['concept:name', 'next_activity', 'elapsed_time']].dropna()
    
    # Calculate median elapsed time for each transition
    median_elapsed_times = transitions.groupby(["concept:name", "next_activity"])['elapsed_time'].median().reset_index()
    top_transitions = median_elapsed_times.nlargest(3, 'elapsed_time')
    
    # Prepare final answer
    final_answer = {
        'top_transitions': top_transitions.to_dict(orient='records')
    }
    
    # Save final answer to JSON
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))