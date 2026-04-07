import pm4py
import pandas as pd
import os
import json
import networkx as nx
import matplotlib.pyplot as plt

def main():
    event_log = ACTIVE_LOG
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    edge_csv_path = os.path.join(output_dir, 'handover_edge_list.csv')
    png_path = os.path.join(output_dir, 'handover_network.png')

    # Convert event log to DataFrame
    log_df = pm4py.convert_to_dataframe(event_log)

    # Create a directed graph for handover of work
    G = nx.DiGraph()

    # Create edges based on resource handovers
    for case_id, group in log_df.groupby('case:concept:name'):
        resources = group['org:resource'].tolist()
        for i in range(len(resources) - 1):
            G.add_edge(resources[i], resources[i + 1])

    # Calculate weights for edges
    edge_weights = G.edges(data=True)
    for u, v, data in edge_weights:
        data['weight'] = G[u][v].get('weight', 0) + 1

    # Prepare edge list for CSV
    edge_table = [(u, v, data['weight']) for u, v, data in G.edges(data=True)]
    edge_df = pd.DataFrame(edge_table, columns=['source', 'target', 'weight'])
    edge_df.to_csv(edge_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {edge_csv_path}')  

    # Visualize the network
    pos = nx.spring_layout(G)
    plt.figure(figsize=(12, 8))
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold', arrows=True)
    edge_labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    plt.title('Handover of Work Network')
    plt.savefig(png_path)
    plt.close()
    print(f'OUTPUT_FILE_LOCATION: {png_path}')  

    # Get top-3 handovers by weight
    top_handover = edge_df.nlargest(3, 'weight')
    strongest_pairs = top_handover.to_dict(orient='records')

    # Prepare final answer
    final_answer = {
        'top_handover': strongest_pairs
    }
    result_csv_path = os.path.join(output_dir, 'result.json')
    with open(result_csv_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {result_csv_path}')  

    print(json.dumps(final_answer, ensure_ascii=False))