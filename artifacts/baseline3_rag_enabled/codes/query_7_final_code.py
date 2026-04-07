import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    log_df = log_df.sort_values(['case:concept:name', 'time:timestamp'])

    # Compute variants using PM4Py's built-in functionality
    variants_raw = pm4py.get_variants(event_log)
    variants_count = {variant: count for variant, count in variants_raw.items()}

    # Count how many variants occur exactly once
    variants_once = {variant: count for variant, count in variants_count.items() if count == 1}
    variants_once_count = len(variants_once)

    # Prepare final answer
    final_answer = {"variants_occurring_once_count": variants_once_count}

    # Save the final answer to a JSON file
    output_path = 'output/variants_once_count.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')  

    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))

if __name__ == '__main__':
    main()