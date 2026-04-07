def main():
    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    fit_rate = pm4py.token_based_replay(ocel, ocpn)
    fit_percentage = fit_rate['fit'] / fit_rate['total'] * 100 if fit_rate['total'] > 0 else 0.0
    with open('output/ocpn_fit_rate.json', 'w', encoding='utf-8') as f:
        json.dump({'fit_percentage': fit_percentage}, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocpn_fit_rate.json')
    final_answer = {'conformance': {'fit_percentage': fit_percentage}}
    print(json.dumps(final_answer, ensure_ascii=False))