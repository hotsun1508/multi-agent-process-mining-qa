def main():
    event_log = ACTIVE_LOG
    # Compute the number of unique trace variants
    variants_raw = pm4py.get_variants(event_log)
    unique_variant_count = len(variants_raw)
    
    # Construct the result dictionary with the required structure
    result = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'event_log',
        'result_schema': {
            'behavior_variant': 'variant_summary'
        },
        'behavior_variant': unique_variant_count
    }
    
    # Save the result as a JSON file
    with open('output/result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result.json')
    
    # Return the final answer
    print(json.dumps(result, ensure_ascii=False))