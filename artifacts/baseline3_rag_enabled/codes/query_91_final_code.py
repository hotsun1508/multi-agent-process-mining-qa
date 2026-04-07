import os
import json
import pm4py
import pandas as pd
import pickle

def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for the object type 'packages'
    flattened_log = pm4py.ocel_flattening(ocel, 'packages')
    
    # Step 2: Get the frequency of variants
    variant_counts = flattened_log['concept:name'].value_counts()
    top_20_percent_count = int(len(variant_counts) * 0.2)
    top_variants = variant_counts.nlargest(top_20_percent_count).index.tolist()
    
    # Step 3: Filter the flattened log for top variants
    sublog = flattened_log[flattened_log['concept:name'].isin(top_variants)]
    
    # Step 4: Discover a Petri net from the sublog
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(sublog)
    
    # Step 5: Save the Petri net model
    with open('output/model_top20.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/model_top20.pkl')
    
    # Step 6: Compute token-based replay fitness on the full flattened log
    fitness_results = pm4py.conformance_token_based_replay(flattened_log, petri_net, initial_marking, final_marking)
    
    # Step 7: Save the fitness results to a JSON file
    with open('output/fitness_full.json', 'w') as f:
        json.dump(fitness_results, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_full.json')
    
    # Step 8: Prepare the final benchmark answer
    final_answer = {
        'status': 'success',
        'result_type': 'composite',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {'fitness': fitness_results},
        'artifacts_schema': ['output/*']
    }
    
    # Step 9: Write the final answer to the result CSV/log
    with open('output/result_log.json', 'w') as f:
        json.dump(final_answer, f)
    print(json.dumps(final_answer, ensure_ascii=False))