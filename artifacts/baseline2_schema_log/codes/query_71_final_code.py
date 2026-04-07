import pm4py
import json
import os


def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    
    # Discover the Petri net from the flattened orders log
    ocpn = pm4py.discover_oc_petri_net(ocel)
    reference_net, initial_marking, final_marking = ocpn['petri_nets']['orders'][0]  # Get the Petri net for orders
    
    # Compute token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(flat_orders, reference_net, initial_marking, final_marking)
    
    # Save the fitness result
    fitness_path = 'output/fitness_orders.json'
    with open(fitness_path, 'w', encoding='utf-8') as f:
        json.dump(fitness, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {fitness_path}')  
    
    # Prepare the final answer
    final_answer = {'conformance': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()