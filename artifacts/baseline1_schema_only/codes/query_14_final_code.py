import pandas as pd
import json
import os
import matplotlib.pyplot as plt


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    log_df['date'] = log_df['time:timestamp'].dt.date

    # Count events per day
    daily_event_counts = log_df['date'].value_counts().sort_index()

    # Create Dotted Chart
    plt.figure(figsize=(12, 6))
    plt.scatter(daily_event_counts.index, daily_event_counts.values, color='blue')
    plt.title('Distribution of Events Over Time')
    plt.xlabel('Date')
    plt.ylabel('Event Count')
    plt.xticks(rotation=45)
    plt.grid()
    plt.tight_layout()
    chart_path = 'output/dotted_chart.png'
    plt.savefig(chart_path)
    plt.close()
    print(f'OUTPUT_FILE_LOCATION: {chart_path}')  

    # Get top-3 busiest time windows
    top_3_busiest_days = daily_event_counts.nlargest(3)
    top_3_busiest_days_dict = top_3_busiest_days.to_dict()

    # Prepare final answer
    final_answer = {'top_3_busiest_days': top_3_busiest_days_dict}
    with open('output/benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    print(json.dumps(final_answer, ensure_ascii=False))