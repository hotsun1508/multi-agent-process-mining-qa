import pm4py
import json
import os
import pickle


def main():
    event_log = ACTIVE_LOG
    # Discover Petri net using Inductive Miner
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
    # Save the Petri net visualization
    petri_net_path = 'output/petri_net.png'
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, petri_net_path)
    print(f'OUTPUT_FILE_LOCATION: {petri_net_path}')  
    # Save the Petri net model
    with open('output/petri_net.pkl', 'wb') as f:
        pickle.dump((petri_net, initial_marking, final_marking), f)
    print('OUTPUT_FILE_LOCATION: output/petri_net.pkl')
    # Calculate token-based replay fitness
    fitness = pm4py.fitness_token_based_replay(event_log, petri_net, initial_marking, final_marking)
    # Prepare final answer
    final_answer = {'fitness': fitness}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()