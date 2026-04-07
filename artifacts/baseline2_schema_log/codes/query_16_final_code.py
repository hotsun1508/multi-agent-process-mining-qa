import pm4py
import pandas as pd
import json

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    log_df["time:timestamp"] = pd.to_datetime(log_df["time:timestamp"])
    log_df["sojourn_time"] = log_df.groupby("case:concept:name")["time:timestamp"].diff().dt.total_seconds().fillna(0)
    median_sojourn_times = log_df.groupby("concept:name")["sojourn_time"].median().sort_values(ascending=False)
    longest_activity = median_sojourn_times.idxmax()
    longest_median_time = median_sojourn_times.max()
    final_answer = {"activity": longest_activity, "median_sojourn_time": longest_median_time}
    with open('output/longest_median_sojourn_time.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/longest_median_sojourn_time.json")
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == "__main__":
    main()