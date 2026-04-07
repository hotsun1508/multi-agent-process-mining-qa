import pm4py
import pandas as pd
import json
import os
from collections import defaultdict


def main():
    event_log = ACTIVE_LOG
    log_df = pm4py.convert_to_dataframe(event_log).sort_values(["case:concept:name", "time:timestamp"])
    handover_network = defaultdict(int)

    # Compute handover of work
    for case_id, group in log_df.groupby("case:concept:name"):
        resources = group["org:resource"].tolist()
        for i in range(len(resources) - 1):
            handover_network[(resources[i], resources[i + 1])] += 1

    # Prepare data for visualization and CSV
    handover_df = pd.DataFrame(handover_network.items(), columns=["edge", "weight"])
    handover_df[['source', 'target']] = handover_df['edge'].apply(pd.Series)
    handover_df = handover_df.drop(columns=['edge'])

    # Save weighted edge list as CSV
    csv_path = "output/handover_work_network.csv"
    handover_df.to_csv(csv_path, index=False)
    print(f"OUTPUT_FILE_LOCATION: {csv_path}")

    # Get top-3 handovers by weight
    top_handover = handover_df.nlargest(3, 'weight')
    top_handover_list = top_handover[['source', 'target', 'weight']].to_dict(orient='records')

    # Visualization
    import networkx as nx
    import matplotlib.pyplot as plt

    G = nx.DiGraph()
    for _, row in handover_df.iterrows():
        G.add_edge(row['source'], row['target'], weight=row['weight'])

    pos = nx.spring_layout(G)
    weights = [G[u][v]['weight'] for u, v in G.edges()]
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold', arrows=True)
    nx.draw_networkx_edge_labels(G, pos, edge_labels={(u, v): G[u][v]['weight'] for u, v in G.edges()})
    plt.title('Handover of Work Network')
    png_path = "output/handover_work_network.png"
    plt.savefig(png_path)
    plt.close()
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Final answer
    final_answer = {"top_handover": top_handover_list}
    print(json.dumps(final_answer, ensure_ascii=False))


if __name__ == '__main__':
    main()