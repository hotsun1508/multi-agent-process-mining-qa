import pandas as pd
import pm4py
import matplotlib.pyplot as plt
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    log_df.set_index('time:timestamp', inplace=True)
    
    # Create a Dotted Chart
    plt.figure(figsize=(12, 6))
    plt.scatter(log_df.index, [1] * len(log_df), alpha=0.5)
    plt.title('Distribution of Events Over Time')
    plt.xlabel('Time')
    plt.yticks([])
    plt.grid()
    plt.savefig('output/dotted_chart.png')
    print('OUTPUT_FILE_LOCATION: output/dotted_chart.png')
    
    # Count events per day
    events_per_day = log_df.resample('D').size()
    top_3_busiest_days = events_per_day.nlargest(3)
    top_3_busiest_days_dict = top_3_busiest_days.to_dict()
    
    # Prepare final answer
    final_answer = {'top_3_busiest_days': top_3_busiest_days_dict}
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))