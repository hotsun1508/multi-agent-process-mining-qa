def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    start_activities = log_df.groupby("case:concept:name")['concept:name'].first().value_counts().to_dict()
    end_activities = log_df.groupby("case:concept:name")['concept:name'].last().value_counts().to_dict()
    unique_start_activity = list(start_activities.keys())[0]  # Most common start activity
    unique_end_activity = list(end_activities.keys())[0]  # Most common end activity
    final_answer = {"start_activity": unique_start_activity, "end_activity": unique_end_activity}
    with open('output/final_benchmark_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_benchmark_answer.json")
    print(json.dumps(final_answer, ensure_ascii=False))