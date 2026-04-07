import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    # Group by case ID and get the sequence of activities for each case
    variants = log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).value_counts()
    # Count how many variants occur exactly once
    single_occurrence_variants = variants[variants == 1].count()
    
    # Prepare the final answer
    final_answer = {'exactly_once_variants': int(single_occurrence_variants)}
    
    # Save the final answer to a JSON file
    with open('output/variant_count.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/variant_count.json')
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()