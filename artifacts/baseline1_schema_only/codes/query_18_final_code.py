import pm4py
import pandas as pd
import json
import os
from collections import defaultdict
import matplotlib.pyplot as plt
import networkx as nx

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
    handover_df = pd.DataFrame(handover_network.items(), columns=["source", "target", "weight"])
    handover_df["weight"] = handover_df["weight"].astype(int)
    top_handover = handover_df.nlargest(3, "weight")

    # Save weighted edge list as CSV
    csv_path = "output/handover_work_network.csv"
    handover_df.to_csv(csv_path, index=False)
    print(f"OUTPUT_FILE_LOCATION: {csv_path}")

    # Visualization
    G = nx.DiGraph()
    for (src, tgt), weight in handover_network.items():
        G.add_edge(src, tgt, weight=weight)

    pos = nx.spring_layout(G)
    edges = G.edges(data=True)
    weights = [edge[2]['weight'] for edge in edges]
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold', arrows=True)
    nx.draw_networkx_edge_labels(G, pos, edge_labels={(src, tgt): weight for (src, tgt, weight) in edges})

    # Save visualization as PNG
    png_path = "output/handover_work_network.png"
    plt.savefig(png_path)
    plt.close()
    print(f"OUTPUT_FILE_LOCATION: {png_path}")

    # Final answer
    final_answer = {
        "top_handover": top_handover.to_dict(orient='records')
    }
    print(json.dumps(final_answer, ensure_ascii=False))