import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    log_df["time:timestamp"] = pd.to_datetime(log_df["time:timestamp"])
    log_df["sojourn_time"] = log_df.groupby("case:concept:name")[
        "time:timestamp"].transform(lambda x: x.max() - x.min()).dt.total_seconds()
    avg_sojourn_time = log_df.groupby("concept:name")[
        "sojourn_time"].mean().sort_values(ascending=False)
    longest_activity = avg_sojourn_time.idxmax()
    top_resources = log_df[log_df["concept:name"] == longest_activity]["org:resource"].value_counts().head(5).to_dict()
    final_answer = {"longest_activity": longest_activity, "top_resources": top_resources}
    os.makedirs("output", exist_ok=True)
    with open("output/benchmark_result.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f"OUTPUT_FILE_LOCATION: output/benchmark_result.json")
    print(json.dumps(final_answer, ensure_ascii=False))