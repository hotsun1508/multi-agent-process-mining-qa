import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    
    # Calculate the Working Together metric
    collaboration_counts = log_df.groupby(["case:concept:name", "org:resource"]).size().reset_index(name='counts')
    collaboration_matrix = collaboration_counts.pivot(index='case:concept:name', columns='org:resource', values='counts').fillna(0)
    working_together = collaboration_matrix.T.dot(collaboration_matrix)
    np.fill_diagonal(working_together.values, 0)  # Remove self-collaboration counts
    
    # Get top 3 collaborating resources
    top_resources = working_together.sum(axis=1).nlargest(3).index.tolist()
    
    # Filter cases involving all three resources
    filtered_cases = log_df[log_df["org:resource"].isin(top_resources)]
    case_counts = filtered_cases.groupby("case:concept:name").size().reset_index(name='counts')
    dominant_variant = case_counts[case_counts['counts'] == case_counts['counts'].max()]
    dominant_variant_str = dominant_variant['case:concept:name'].values[0] if not dominant_variant.empty else None
    
    # Prepare final answer
    final_answer = {
        "top_resources": top_resources,
        "dominant_variant": dominant_variant_str
    }
    
    # Save final answer to JSON
    with open("output/final_answer.json", "w", encoding="utf-8") as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/final_answer.json")
    
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()