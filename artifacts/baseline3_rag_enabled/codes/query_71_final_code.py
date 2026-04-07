import os
import json
import pm4py


def main():
    ocel = ACTIVE_LOG
    # Ensure output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Flatten the OCEL for the object type 'orders'
    flattened_log = pm4py.ocel_flattening(ocel, 'orders')

    # Step 2: Discover the Petri net from the flattened log
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(flattened_log)

    # Step 3: Compute token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(flattened_log, petri_net, initial_marking, final_marking)

    # Step 4: Save the fitness result
    with open(os.path.join(output_dir, 'fitness_orders.json'), 'w') as f:
        json.dump(fitness, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/fitness_orders.json')

    # Step 5: Prepare the final answer
    final_answer = {'conformance': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()