import os
import json
import pm4py
import pandas as pd

def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Convert to dataframe
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df['time:timestamp'] = pd.to_datetime(log_df['time:timestamp'])
    log_df['date'] = log_df['time:timestamp'].dt.date

    # Count events per day
    daily_counts = log_df.groupby('date').size().reset_index(name='event_count')
    top_3_busiest_days = daily_counts.nlargest(3, 'event_count')

    # Generate Dotted Chart
    dotted_chart_path = os.path.join(output_dir, 'dotted_chart.png')
    ax = daily_counts.plot.scatter(x='date', y='event_count', title='Dotted Chart of Events Over Time')
    ax.set_xlabel('Date')
    ax.set_ylabel('Event Count')
    ax.figure.savefig(dotted_chart_path)
    print(f'OUTPUT_FILE_LOCATION: {dotted_chart_path}')  

    # Prepare final answer
    final_answer = {
        'top_3_busiest_days': top_3_busiest_days.to_dict(orient='records')
    }
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()