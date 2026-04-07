import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    variants_count = log_df.groupby(["case:concept:name"])['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    variants_once = variants_count[variants_count == 1].count()
    final_answer = {"variants_occur_once": int(variants_once)}
    print(json.dumps(final_answer, ensure_ascii=False))

    # Save the final answer to a JSON file
    output_path = 'output/variants_once.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  


if __name__ == '__main__':
    main()