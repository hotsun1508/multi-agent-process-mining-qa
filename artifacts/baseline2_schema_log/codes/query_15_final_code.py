import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    
    # Generate Performance Spectrum visualization
    performance_spectrum_path = 'output/performance_spectrum.png'
    pm4py.visualization.performance_spectrum.apply(event_log, parameters={'output': performance_spectrum_path})
    print(f'OUTPUT_FILE_LOCATION: {performance_spectrum_path}')  
    
    # Calculate elapsed times between activity transitions
    log_df['elapsed_time'] = log_df.groupby('case:concept:name')['time:timestamp'].diff().dt.total_seconds()
    transitions = log_df[['concept:name', 'elapsed_time']].dropna()
    transitions['next_activity'] = transitions['concept:name'].shift(-1)
    transitions = transitions[transitions['next_activity'].notnull()]
    
    # Calculate median elapsed time for each transition
    median_elapsed_time = transitions.groupby(['concept:name', 'next_activity'])['elapsed_time'].median().reset_index()
    median_elapsed_time.columns = ['source', 'target', 'median_elapsed_time']
    top_transitions = median_elapsed_time.nlargest(3, 'median_elapsed_time')
    
    # Prepare final answer
    final_answer = {'top_transitions': top_transitions.to_dict(orient='records')}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()