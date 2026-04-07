import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    # Calculate throughput time for each case
    log_df["throughput_time"] = log_df.groupby("case:concept:name")["time:timestamp"].transform(lambda x: (x.max() - x.min()).total_seconds())
    # Get the frequency of each variant
    variants = log_df.groupby(["case:concept:name", "concept:name"]).size().reset_index(name='frequency')
    # Get the top 1% most frequent variants
    top_1_percent_threshold = variants["frequency"].quantile(0.99)
    top_variants = variants[variants["frequency"] >= top_1_percent_threshold]
    # Calculate median throughput time for each variant
    median_throughput = log_df.groupby(["case:concept:name"])['throughput_time'].median().reset_index()
    # Merge to find the variant with the highest median throughput time
    merged = pd.merge(top_variants, median_throughput, on="case:concept:name")
    highest_median_variant = merged.loc[merged["throughput_time"].idxmax()]
    variant_name = highest_median_variant["case:concept:name"]
    # Get the top 3 resources for this variant
    top_resources = log_df[log_df["case:concept:name"] == variant_name]["org:resource"].value_counts().head(3).index.tolist()
    # Prepare final answer
    final_answer = {
        "behavior_variant": variant_name,
        "performance": highest_median_variant["throughput_time"],
        "resource": top_resources
    }
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    print(json.dumps(final_answer, ensure_ascii=False))