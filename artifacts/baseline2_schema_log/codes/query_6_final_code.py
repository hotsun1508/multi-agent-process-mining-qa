import pm4py
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    variants = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    most_frequent_variant = variants.idxmax()
    frequency = variants.max()
    final_answer = {"most_frequent_variant": most_frequent_variant, "frequency": int(frequency)}

    # Save the final answer to a JSON file
    output_path = 'output/frequent_variant.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()