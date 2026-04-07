import pm4py
import pandas as pd
import json
import os

def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    resource_counts = log_df['org:resource'].value_counts().head(5)
    top_resources = resource_counts.index.tolist()
    final_answer = {'top_resources': top_resources}
    print(json.dumps(final_answer, ensure_ascii=False))
    
    # Save the final answer to a JSON file
    output_path = 'output/top_resources.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {output_path}')


if __name__ == '__main__':
    main()