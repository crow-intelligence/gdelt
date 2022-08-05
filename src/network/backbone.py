import networkx as nx

from src.network.disparity import calc_alpha_ptile, cut_graph, disparity_filter

G = nx.read_graphml("data/graphs/full_graph.graphml")
print(len(G.nodes), len(G.edges))
alpha_measures = disparity_filter(G)
quantiles, num_quant = calc_alpha_ptile(alpha_measures)
alpha_cutoff = quantiles[5]

cut_graph(G, alpha_cutoff, 3)

nx.write_graphml(G, "data/graphs/backbone3.graphml")
print(len(G.nodes), len(G.edges))
