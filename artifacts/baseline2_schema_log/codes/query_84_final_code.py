def main():
    ocel = ACTIVE_LOG
    flat_items = pm4py.ocel_flattening(ocel, object_type='items')
    petri_net = pm4py.discover_petri_net_inductive(flat_items)
    png_path = 'output/im_items.png'
    pm4py.save_vis_petri_net(petri_net, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    with open('output/im_items.pkl', 'wb') as f:
        pickle.dump(petri_net, f)
    print('OUTPUT_FILE_LOCATION: output/im_items.pkl')
    fitness = pm4py.fitness_token_based_replay(petri_net, flat_items)
    with open('output/fitness_im_items.json', 'w', encoding='utf-8') as f:
        json.dump(fitness, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/fitness_im_items.json')
    final_answer = {'petri_net': {'places': len(petri_net.places), 'transitions': len(petri_net.transitions)}, 'fitness': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))