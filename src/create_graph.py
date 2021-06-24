import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import write_dot
import run_program
import sys
import os

def create_graph(marginal_df, stopping_prob):
    G = nx.MultiDiGraph()
    edge_labels={}

    for o_state in marginal_df.columns:
        for d_state in marginal_df.columns:
            rate = marginal_df.at[d_state, o_state]
            if rate > stopping_prob and rate < 1:
                G.add_edge(o_state,
                        d_state,
                        weight=rate,
                        label="{:.02f}".format(rate))
                edge_labels[(o_state, d_state)] = label="{:.02f}".format(rate)
    return G 

def draw_graph(project_path):
    output_path = os.sep.join([project_path, 'output'])
    dot_path = os.sep.join([output_path, 'mc.dot'])
    graph_path = os.sep.join([output_path, 'graph.png'])
    cmd = "dot -Tpng {} -o {}".format(dot_path, graph_path)
    os.system(cmd)

def main():
    project=sys.argv[1]
    stopping_prob=float(sys.argv[2])
    project_path = os.sep.join(['..', 'runs', project])
    metadata, hashtags_searched, tweet_df, marginal_df = run_program.load_data(project_path)
    G = create_graph(marginal_df, stopping_prob)
    output_path = os.sep.join([project_path, 'output', 'mc.dot'])
    write_dot(G, output_path)
    draw_graph(project_path)
    
if __name__ == "__main__":
    main()


# python -m pip install --global-option=build_ext --global-option="-IC:\Program Files\Graphviz\include" --global-option="-LC:\Program Files\Graphviz\lib" pygraphviz
# dot -Tpng 'mc.dot' -o 'graph.png'