import pm4py
import json

def main():
    event_log = ACTIVE_LOG
    # Convert the event log to a dataframe
    log_df = pm4py.convert_to_dataframe(event_log)
    # Group by case ID and get unique trace variants
    unique_variants = log_df.groupby('case:concept:name')['concept:name'].apply(lambda x: ' -> '.join(x)).unique()
    total_unique_variants = len(unique_variants)
    
    # Prepare the final answer
    final_answer = {'total_unique_trace_variants': total_unique_variants}
    
    # Save the final answer to a JSON file
    with open('output/unique_trace_variants.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/unique_trace_variants.json')
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))