import pm4py
import json
import os

def main():
    ocel = ACTIVE_LOG
    # Step 1: Load the reference OC-Petri net
    ocpn = pm4py.discover_oc_petri_net(ocel)
    
    # Step 2: Compute the object-centric token-based replay fitness
    fitness_results = pm4py.conformance.token_replay_fitness_tool(ocel, ocpn)
    fit_rate = fitness_results['fit'] / fitness_results['total'] * 100 if fitness_results['total'] > 0 else 0
    
    # Step 3: Save the computed fitness results to a JSON file
    with open('output/ocpn_fit_rate.json', 'w') as json_file:
        json.dump({'fit_rate': fit_rate}, json_file)
    print('OUTPUT_FILE_LOCATION: output/ocpn_fit_rate.json')
    
    # Step 4: Prepare the final benchmark answer
    result_dict = {
        'status': 'success',
        'result_type': 'single',
        'view': 'raw_ocel',
        'result': {
            'conformance': fit_rate
        },
        'artifacts': []
    }
    
    # Step 5: Write the final answer to the result CSV/log
    with open('output/result_log.json', 'w') as log_file:
        json.dump(result_dict, log_file)
    print('OUTPUT_FILE_LOCATION: output/result_log.json')
    
    # Final output
    print(json.dumps(result_dict, ensure_ascii=False))