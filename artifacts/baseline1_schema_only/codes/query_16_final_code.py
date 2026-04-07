import pm4py
import pandas as pd
import json


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    log_df["time:timestamp"] = pd.to_datetime(log_df["time:timestamp"])
    log_df["sojourn_time"] = log_df.groupby("case:concept:name")["time:timestamp"].diff().dt.total_seconds().fillna(0)
    median_sojourn_times = log_df.groupby("concept:name")["sojourn_time"].median().reset_index()
    longest_activity = median_sojourn_times.loc[median_sojourn_times["sojourn_time"].idxmax()]
    final_answer = {"activity": longest_activity["concept:name"], "median_sojourn_time": longest_activity["sojourn_time"]}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == "__main__":
    main()