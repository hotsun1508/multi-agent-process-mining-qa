import os
import pandas as pd
import pm4py
import json

def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'customers'
    flattened_log = pm4py.ocel_flattening(ocel, 'customers')

    # Step 2: Compute the variant frequency table
    variant_counts = flattened_log['concept:name'].value_counts().reset_index()
    variant_counts.columns = ['variant', 'frequency']

    # Step 3: Save the variant frequency table as CSV
    variants_csv_path = os.path.join(output_dir, 'variants_customers.csv')
    variant_counts.to_csv(variants_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {variants_csv_path}')  

    # Step 4: Prepare the final benchmark answer
    n_variants = variant_counts.shape[0]
    final_answer = {'unique_trace_variants': n_variants}
    print(json.dumps(final_answer, ensure_ascii=False))

if __name__ == '__main__':
    main()