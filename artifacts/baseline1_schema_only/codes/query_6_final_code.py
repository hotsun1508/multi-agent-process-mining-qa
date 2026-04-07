import pm4py
import json

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    variants = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variant = variants.idxmax()
    frequency = variants.max()
    final_answer = {"most_frequent_variant": most_frequent_variant, "frequency": int(frequency)}
    with open('output/most_frequent_variant.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print("OUTPUT_FILE_LOCATION: output/most_frequent_variant.json")
    print(json.dumps(final_answer, ensure_ascii=False))