import pandas as pd
import json
from pm4py.objects.log.util import dataframe_utils


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    co_occurrence_matrix = pd.DataFrame(0, index=log_df["org:resource"].unique(), columns=log_df["org:resource"].unique())

    # Compute co-occurrence for each case
    for case_id, group in log_df.groupby("case:concept:name"):
        resources = group["org:resource"].unique()
        for i in range(len(resources)):
            for j in range(i, len(resources)):
                co_occurrence_matrix.loc[resources[i], resources[j]] += 1
                if i != j:
                    co_occurrence_matrix.loc[resources[j], resources[i]] += 1

    # Save the co-occurrence matrix to CSV
    co_occurrence_matrix_path = "output/co_occurrence_matrix.csv"
    co_occurrence_matrix.to_csv(co_occurrence_matrix_path)
    print(f"OUTPUT_FILE_LOCATION: {co_occurrence_matrix_path}")

    # Prepare final answer
    final_answer = {"co_occurrence_matrix": co_occurrence_matrix.to_dict()}
    print(json.dumps(final_answer, ensure_ascii=False))