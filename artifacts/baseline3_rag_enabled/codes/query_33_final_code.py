import os
import json
import pandas as pd
import pm4py

def main():
    event_log = ACTIVE_LOG
    # Step 1: Convert the event log to a case dataframe
    df = pm4py.convert_to_dataframe(event_log)
    
    # Step 2: Get the variants and their frequencies
    variants = pm4py.get_variants(event_log)
    variant_counts = sorted(variants.items(), key=lambda x: len(x[1]), reverse=True)
    top_50_percent_index = len(variant_counts) // 2
    top_variants = variant_counts[:top_50_percent_index]
    top_variant_cases = set()
    
    # Collect cases belonging to the top 50% variants
    for variant, cases in top_variants:
        top_variant_cases.update(cases)
    
    # Filter the original dataframe to only include top variant cases
    filtered_df = df[df['case:concept:name'].isin(top_variant_cases)]
    
    # Step 3: Discover a Petri net using the Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_df)
    
    # Step 4: Calculate the token-based replay fitness
    fitness_results = pm4py.conformance_diagnostics_token_based_replay(filtered_df, petri_net, initial_marking, final_marking)
    fitting_traces = sum(1 for result in fitness_results if result['trace_is_fit'])
    total_traces = len(fitness_results)
    percentage_of_fitting_traces = (fitting_traces / total_traces) * 100 if total_traces > 0 else 0
    
    # Step 5: Prepare the result dictionary
    result_dict = {
        'top_50_percent_variants_count': top_50_percent_index,
        'fitting_traces': fitting_traces,
        'total_traces': total_traces,
        'percentage_of_fitting_traces': percentage_of_fitting_traces
    }
    
    # Step 6: Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    
    # Step 7: Save the result dictionary to a CSV/log file
    result_csv_path = 'output/benchmark_results.csv'
    pd.DataFrame([result_dict]).to_csv(result_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {result_csv_path}')  
    
    # Final answer
    print(json.dumps(result_dict, ensure_ascii=False))