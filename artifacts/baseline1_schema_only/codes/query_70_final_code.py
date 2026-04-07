def main():
    import json
    import pm4py
    import os

    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    fit_rate = pm4py.replay_fitness(ocel, ocpn)
    fit_percentage = fit_rate['fit'] / fit_rate['total'] * 100 if fit_rate['total'] > 0 else 0.0

    # Save fit rate to JSON file
    fit_rate_output = {'fit_rate_percentage': fit_percentage}
    os.makedirs('output', exist_ok=True)
    with open('output/ocpn_fit_rate.json', 'w', encoding='utf-8') as f:
        json.dump(fit_rate_output, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocpn_fit_rate.json')

    # Prepare final answer
    final_answer = {'conformance': {'fit_rate_percentage': fit_percentage}}
    print(json.dumps(final_answer, ensure_ascii=False))