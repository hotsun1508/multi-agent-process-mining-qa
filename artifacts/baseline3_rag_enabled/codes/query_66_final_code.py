import os
import json
import pandas as pd
from pm4py import ocel_flattening


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'items'
    try:
        flattened_log = ocel_flattening(ocel, 'items')
    except Exception as e:
        final_answer = {
            'top1_covers20': False,
            '__summary__': {
                'note': f'Failed to flatten OCEL: {str(e)}'
            }
        }
        print(json.dumps(final_answer, ensure_ascii=False))
        return
    
    # Step 2: Calculate the number of cases and the frequency of variants
    case_counts = flattened_log['case:concept:name'].nunique()
    variant_counts = flattened_log['concept:name'].value_counts()
    
    # Step 3: Determine the top 1% variants by frequency
    top_1_percent_count = max(1, int(len(variant_counts) * 0.01))
    top_variants = variant_counts.nlargest(top_1_percent_count)
    
    # Step 4: Calculate the coverage of these top variants
    top_variants_coverage = top_variants.sum() / len(flattened_log)
    top1_covers20 = top_variants_coverage > 0.2
    
    # Step 5: Save the result as JSON
    with open(os.path.join(output_dir, 'top1_covers20_items.json'), 'w') as f:
        json.dump({'top1_covers20': top1_covers20}, f)
    print(f'OUTPUT_FILE_LOCATION: {os.path.join(output_dir, "top1_covers20_items.json")}')  
    
    # Step 6: Prepare the final benchmark answer
    final_answer = {
        'top1_covers20': top1_covers20
    }
    print(json.dumps(final_answer, ensure_ascii=False))