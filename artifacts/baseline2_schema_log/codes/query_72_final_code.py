def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL for customers
    flattened_customers = pm4py.ocel_flattening(ocel, object_type='customers')
    # Discover the Petri net for the flattened view
    reference_petri_net = pm4py.discover_oc_petri_net(ocel)
    # Get the variants and their frequencies
    variants = flattened_customers['concept:name'].value_counts().head(int(len(flattened_customers) * 0.2)).index.tolist()
    # Filter the log for the top 20% variants
    filtered_log = flattened_customers[flattened_customers['concept:name'].isin(variants)]
    # Compute the fit rate under token-based replay
    fit_cases = 0
    total_cases = len(filtered_log['case:concept:name'].unique())
    for case in filtered_log['case:concept:name'].unique():
        case_log = filtered_log[filtered_log['case:concept:name'] == case]
        # Token-based replay
        fitness = pm4py.fitness_token_based_replay(case_log, reference_petri_net)
        if fitness == 0:
            fit_cases += 1
    fit_rate = fit_cases / total_cases if total_cases > 0 else 0.0
    # Save the fit rate result
    fit_rate_result = {'fit_rate': fit_rate}
    with open('output/fit_rate_tbr_customers.json', 'w') as f:
        json.dump(fit_rate_result, f)
    print('OUTPUT_FILE_LOCATION: output/fit_rate_tbr_customers.json')
    # Prepare final answer
    final_answer = {'conformance': fit_rate}
    print(json.dumps(final_answer, ensure_ascii=False))