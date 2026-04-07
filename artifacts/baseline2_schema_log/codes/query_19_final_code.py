import pm4py
import pandas as pd
import json
import os


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    # Create a DataFrame for the collaboration network
    collaboration_df = log_df.groupby(['org:resource', 'case:concept:name']).size().reset_index(name='weight')
    # Create a weighted edge list
    edge_list = collaboration_df.groupby(['org:resource']).agg({'weight': 'sum'}).reset_index()
    edge_list.to_csv('output/weighted_edge_list.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/weighted_edge_list.csv')
    # Create the social network visualization
    collaboration_network = collaboration_df.pivot(index='org:resource', columns='case:concept:name', values='weight').fillna(0)
    collaboration_network = collaboration_network.astype(int)
    plt.figure(figsize=(10, 8))
    plt.title('Working Together Social Network')
    plt.imshow(collaboration_network, cmap='Blues')
    plt.colorbar(label='Weight')
    plt.xticks(range(len(collaboration_network.columns)), collaboration_network.columns, rotation=90)
    plt.yticks(range(len(collaboration_network.index)), collaboration_network.index)
    plt.savefig('output/working_together_network.png')
    print('OUTPUT_FILE_LOCATION: output/working_together_network.png')
    # Get top-3 collaborating pairs by weight
    top_collaborations = collaboration_df.nlargest(3, 'weight')
    top_collaborating_pairs = top_collaborations[['org:resource', 'weight']].to_dict(orient='records')
    final_answer = {'top_collaborating_pairs': top_collaborating_pairs}
    with open('output/final_answer.json', 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print('OUTPUT_FILE_LOCATION: output/final_answer.json')
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()