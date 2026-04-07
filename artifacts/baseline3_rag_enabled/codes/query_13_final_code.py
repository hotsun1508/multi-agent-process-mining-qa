import os
import json
import pm4py
import pandas as pd
import numpy as np


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(['case:concept:name', 'time:timestamp'])
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    throughput_times = log_df.groupby('case:concept:name').agg(start_time=('time:timestamp', 'min'), end_time=('time:timestamp', 'max'))
    throughput_times['throughput_time'] = (throughput_times['end_time'] - throughput_times['start_time']).dt.total_seconds()
    average_throughput_time = throughput_times['throughput_time'].mean()
    median_throughput_time = throughput_times['throughput_time'].median()
    final_answer = {'average_throughput_time': average_throughput_time, 'median_throughput_time': median_throughput_time}
    with open('output/throughput_times.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/throughput_times.json')
    print(json.dumps(final_answer, ensure_ascii=False))