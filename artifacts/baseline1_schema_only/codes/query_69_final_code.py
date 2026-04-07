def main():
    ocel = ACTIVE_LOG
    ocpn = pm4py.discover_oc_petri_net(ocel)
    fitness = pm4py.replay_fitness_token_based(ocel, ocpn)
    with open('output/ocpn_fitness.json', 'w', encoding='utf-8') as f:
        json.dump(fitness, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocpn_fitness.json')
    final_answer = {'conformance': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))