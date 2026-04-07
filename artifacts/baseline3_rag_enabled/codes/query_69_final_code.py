def main():
    ocel = ACTIVE_LOG
    
    # Step 1: Load the reference OC-Petri net
    ocpn = pm4py.discover_oc_petri_net(ocel)
    png_path = 'output/ocpn_visualization.png'
    pm4py.save_vis_ocpn(ocpn, png_path)
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    
    # Step 2: Compute the object-centric token-based replay fitness
    fitness_results = pm4py.conformance.token_replay_fitness(ocel, ocpn)
    
    # Step 3: Save the computed fitness results to a JSON file
    with open('output/ocpn_fitness.json', 'w', encoding='utf-8') as f:
        json.dump(fitness_results, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/ocpn_fitness.json')
    
    # Step 4: Prepare the final benchmark answer
    result_dict = {
        'status': 'success',
        'result_type': 'single',
        'view': 'raw_ocel',
        'result': {
            'conformance': fitness_results
        },
        'artifacts': []
    }
    
    # Step 5: Write the final answer to the result CSV/log
    with open('output/result_log.json', 'w', encoding='utf-8') as f:
        json.dump(result_dict, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/result_log.json')
    
    # Final output
    print(json.dumps(result_dict, ensure_ascii=False))