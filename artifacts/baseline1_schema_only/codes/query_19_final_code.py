import pm4py
import pandas as pd
import json
import os
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log)
    
    # Compute the Working Together social network
    collaboration_counts = defaultdict(int)
    for case_id, group in log_df.groupby('case:concept:name'):
        resources = group['org:resource'].unique()
        for i in range(len(resources)):
            for j in range(i + 1, len(resources)):
                collaboration_counts[(resources[i], resources[j])] += 1
                collaboration_counts[(resources[j], resources[i])] += 1
    
    # Prepare the weighted edge list
    edge_list = pd.DataFrame(list(collaboration_counts.items()), columns=['pair', 'weight'])
    edge_list[['source', 'target']] = pd.DataFrame(edge_list['pair'].tolist(), index=edge_list.index)
    edge_list = edge_list[['source', 'target', 'weight']]
    edge_list.to_csv('output/working_together_edges.csv', index=False)
    print('OUTPUT_FILE_LOCATION: output/working_together_edges.csv')
    
    # Get top-3 collaborating pairs by weight
    top_collaborations = edge_list.nlargest(3, 'weight')
    top_collaborations_list = top_collaborations[['source', 'target', 'weight']].to_dict(orient='records')
    
    # Visualization of the social network
    G = nx.Graph()
    for _, row in edge_list.iterrows():
        G.add_edge(row['source'], row['target'], weight=row['weight'])
    
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
    edge_labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    
    png_path = 'output/working_together_network.png'
    plt.savefig(png_path)
    plt.close()
    print(f'OUTPUT_FILE_LOCATION: {png_path}')
    
    # Final answer
    final_answer = {'top_collaborations': top_collaborations_list}
    print(json.dumps(final_answer, ensure_ascii=False))