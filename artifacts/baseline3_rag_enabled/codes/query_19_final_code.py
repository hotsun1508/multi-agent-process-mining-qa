import os
import json
import pandas as pd
import pm4py
import networkx as nx
import matplotlib.pyplot as plt

def compute_working_together_network(event_log, output_dir='output', resource_key='org:resource', case_id_key='case:concept:name', timestamp_key='time:timestamp'):
    os.makedirs(output_dir, exist_ok=True)

    # Prepare edge list
    df = pm4py.convert_to_dataframe(event_log)
    collaboration_counts = df.groupby([resource_key, case_id_key]).size().reset_index(name='count')
    edge_list = collaboration_counts.groupby([resource_key])['count'].sum().reset_index()
    edge_list.columns = ['source', 'weight']
    edge_list = edge_list[edge_list['weight'] > 0]

    # Create a graph
    G = nx.Graph()
    for _, row in edge_list.iterrows():
        G.add_node(row['source'])
        for _, inner_row in edge_list.iterrows():
            if row['source'] != inner_row['source']:
                G.add_edge(row['source'], inner_row['source'], weight=row['weight'])

    # Save edge list to CSV
    edge_csv_path = os.path.join(output_dir, 'working_together_edge_list.csv')
    edge_list.to_csv(edge_csv_path, index=False)
    print(f'OUTPUT_FILE_LOCATION: {edge_csv_path}')  

    # Compute top-3 collaborations
    top_collaborations = sorted(G.edges(data=True), key=lambda x: x[2]['weight'], reverse=True)[:3]
    top_collab_list = [{'source': src, 'target': tgt, 'weight': data['weight']} for src, tgt, data in top_collaborations]

    # Save network visualization
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
    edge_labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    network_png_path = os.path.join(output_dir, 'working_together_network.png')
    plt.savefig(network_png_path)
    plt.close()
    print(f'OUTPUT_FILE_LOCATION: {network_png_path}')  

    # Prepare final answer
    final_answer = {
        'top_collaborations': top_collab_list
    }
    result_json_path = os.path.join(output_dir, 'result.json')
    with open(result_json_path, 'w', encoding='utf-8') as f:
        json.dump(final_answer, f, ensure_ascii=False, indent=2)
    print(f'OUTPUT_FILE_LOCATION: {result_json_path}')  

    return final_answer

def main():
    event_log = ACTIVE_LOG
    result = compute_working_together_network(event_log)
    print(json.dumps(result, ensure_ascii=False))