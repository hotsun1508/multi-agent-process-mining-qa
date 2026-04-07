import os
import json
import pandas as pd
from pm4py import ocel_flattening

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the 'packages' object type
    flattened_log = ocel_flattening(ocel, 'packages')
    
    # Step 2: Count the frequency of each trace variant
    variant_counts = flattened_log['case:concept:name'].value_counts()
    most_frequent_variant = variant_counts.idxmax()
    frequency = variant_counts.max()
    total_cases = len(flattened_log['case:concept:name'])
    relative_share = frequency / total_cases if total_cases > 0 else 0.0
    
    # Step 3: Prepare the result
    result = {
        'variant': most_frequent_variant,
        'frequency': frequency,
        'relative_share': relative_share
    }
    
    # Save the result to a JSON file
    with open(os.path.join(output_dir, 'most_frequent_variant_packages.json'), 'w') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, 'most_frequent_variant_packages.json')})')
    
    # Step 4: Prepare the final benchmark answer
    final_answer = {
        'behavior_variant': result
    }
    
    # Print the final answer
    print(json.dumps(final_answer, ensure_ascii=False))