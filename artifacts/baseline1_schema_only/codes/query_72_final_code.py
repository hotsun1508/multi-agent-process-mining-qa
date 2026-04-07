def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, 'customers')
    # Discover the Petri net for the flattened view
    ocpn = pm4py.discover_oc_petri_net(ocel)
    reference_net = ocpn['petri_nets']['customers'][0]  # Get the reference Petri net for customers
    # Get the variants and their frequencies
    variants = flattened_customers['case:concept:name'].value_counts().reset_index()
    variants.columns = ['variant', 'frequency']
    # Take the top 20% most frequent variants
    top_20_percent = variants.head(int(len(variants) * 0.2))
    # Compute the fit rate under token-based replay
    fit_count = 0
    total_cases = len(top_20_percent)
    for variant in top_20_percent['variant']:
        # Filter the cases for the current variant
        filtered_cases = flattened_customers[flattened_customers['case:concept:name'] == variant]
        # Check fit under token-based replay
        is_fit = pm4py.token_based_replay(filtered_cases, reference_net)
        if is_fit:
            fit_count += 1
    fit_rate = (fit_count / total_cases) * 100 if total_cases > 0 else 0
    # Save the fit rate result
    fit_rate_result = {'fit_rate': fit_rate}
    with open('output/fit_rate_tbr_customers.json', 'w') as f:
        json.dump(fit_rate_result, f)
    print('OUTPUT_FILE_LOCATION: output/fit_rate_tbr_customers.json')
    # Prepare the final answer
    final_answer = {'conformance': fit_rate}
    print(json.dumps(final_answer, ensure_ascii=False))