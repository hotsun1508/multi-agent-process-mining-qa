import pm4py
import json
import os

def main():
    event_log = ACTIVE_LOG
    # Discover Petri net using Inductive Miner
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')
    # Calculate token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(event_log, net, initial_marking, final_marking)
    # Prepare final answer
    final_answer = {'token_based_replay_fitness': fitness}
    # Save the fitness result as JSON
    with open('output/replay_fitness.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/replay_fitness.json')
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()