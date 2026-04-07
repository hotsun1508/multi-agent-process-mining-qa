import os
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    
    # Step 2: Discover process variants
    variants = pm4py.get_variants(flattened_customers)
    
    # Step 3: Select the top 20% most frequent variants
    total_cases = len(flattened_customers)
    top_20_percent_count = int(total_cases * 0.2)
    top_variants = sorted(variants.items(), key=lambda x: x[1], reverse=True)[:top_20_percent_count]
    
    # Step 4: Create a filtered log for the top variants
    top_variant_names = [variant[0] for variant in top_variants]
    filtered_log = flattened_customers[flattened_customers['case:concept:name'].isin(top_variant_names)]
    
    # Step 5: Discover the reference Petri net
    ocpn = pm4py.discover_oc_petri_net(ocel)
    reference_petri_net = ocpn['petri_nets']['customers'][0]
    
    # Step 6: Perform token-based replay
    fit_results = pm4py.token_based_replay(filtered_log, reference_petri_net)
    fit_cases = sum(1 for result in fit_results if result['fit'])
    fit_rate = fit_cases / len(filtered_log) * 100 if len(filtered_log) > 0 else 0.0
    
    # Step 7: Save the fit rate result
    fit_rate_result = {'fit_rate': fit_rate}
    with open('output/fit_rate_tbr_customers.json', 'w') as f:
        json.dump(fit_rate_result, f)
    print('OUTPUT_FILE_LOCATION: output/fit_rate_tbr_customers.json')
    
    # Step 8: Prepare final benchmark answer
    final_answer = {
        'primary_answer_in_csv_log': True,
        'result_type': 'single',
        'view': 'raw_ocel_or_flattened_view_as_specified',
        'result_schema': {'fit_rate': fit_rate}
    }
    
    # Step 9: Save final answer to CSV/log
    with open('output/benchmark_result.json', 'w') as f:
        json.dump(final_answer, f)
    print('OUTPUT_FILE_LOCATION: output/benchmark_result.json')
    
    print(json.dumps(final_answer, ensure_ascii=False))