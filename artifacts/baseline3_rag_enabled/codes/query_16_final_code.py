def main():
    event_log = ACTIVE_LOG
    import pandas as pd
    from statistics import median

    # Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)

    # Calculate the sojourn times for each activity
    df['next_timestamp'] = df.groupby('case:concept:name')['time:timestamp'].shift(-1)
    df['sojourn_time'] = (df['next_timestamp'] - df['time:timestamp']).dt.total_seconds()
    df = df.dropna(subset=['sojourn_time'])  # Drop NaN values which are the first events in each case

    # Calculate the median sojourn time for each activity
    median_sojourn_times = df.groupby('concept:name')['sojourn_time'].median()

    # Identify the activity with the longest median sojourn time
    longest_median_activity = median_sojourn_times.idxmax()
    longest_median_time = median_sojourn_times.max()

    # Prepare the result dictionary
    result = {
        "primary_answer_in_csv_log": True,
        "result_type": "single",
        "view": "event_log",
        "result_schema": {
            "performance": {
                "activity": longest_median_activity,
                "median_sojourn_time": longest_median_time
            }
        },
        "artifacts_schema": []
    }

    # Save the result to the result CSV/log
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')

    final_answer = {"activity": longest_median_activity, "median_sojourn_time": longest_median_time}
    print(json.dumps(final_answer, ensure_ascii=False))