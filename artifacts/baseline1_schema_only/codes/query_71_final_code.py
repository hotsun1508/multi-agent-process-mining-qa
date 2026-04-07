def main():
    ocel = ACTIVE_LOG
    # Flatten the OCEL to get the orders view
    flat_orders = pm4py.ocel_flattening(ocel, 'orders')
    # Discover the Petri net from the flattened orders view
    ocpn = pm4py.discover_oc_petri_net(ocel)
    # Extract the Petri net for the orders object type
    petri_net, initial_marking, final_marking = ocpn['petri_nets']['orders']
    # Compute token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(flat_orders, petri_net, initial_marking, final_marking)
    # Save the fitness result
    with open('output/fitness_orders.json', 'w', encoding='utf-8') as f:
        json.dump(fitness, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/fitness_orders.json')
    # Prepare the final answer
    final_answer = {'conformance': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))