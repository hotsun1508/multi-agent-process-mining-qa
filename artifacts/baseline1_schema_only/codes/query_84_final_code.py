def main():
    ocel = ACTIVE_LOG
    flat_items = pm4py.ocel_flattening(ocel, 'items')
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flat_items)
    with open('output/im_items.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/im_items.pkl')
    fitness = pm4py.fitness_token_based_replay(flat_items, petri_net, initial_marking, final_marking)
    with open('output/fitness_im_items.json', 'w') as f:
        json.dump(fitness, f)
    print('OUTPUT_FILE_LOCATION: output/fitness_im_items.json')
    final_answer = {'petri_net': {'places': len(petri_net.places), 'transitions': len(petri_net.transitions)}, 'fitness': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))