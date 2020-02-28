import json
import matplotlib.pyplot as plt
import networkx as nx
import os


verts = {}
with open("./swapi_vertices.json") as fh:
    for line in fh:
        line = json.loads(line)
        verts[line["gid"]] = line

edges = {}
with open("./swapi_edges.json") as fh:
    for line in fh:
        line = json.loads(line)
        edges[line["gid"]] = line

G1 = nx.DiGraph()
for gid, e in edges.items():
    G1.add_edge(e["from"], e["to"])

G = nx.DiGraph()
whitelist = list(G1.neighbors("films:1")) + ["films:1"]
edges_sub = []
verts_sub = [verts[x] for x in whitelist]
for gid, e in edges.items():
    if e["from"] not in whitelist or e["to"] not in whitelist:
        continue
    G.add_edge(e["from"], e["to"])
    edges_sub.append(e)

# write subgraph to output files
with open("swapi_subgraph_vertices.json", "w") as fh:
    for v in verts_sub:
        fh.write(json.dumps(v))
        fh.write(os.linesep)

with open("swapi_subgraph_edges.json", "w") as fh:
    for e in edges_sub:
        fh.write(json.dumps(e))
        fh.write(os.linesep)

# Plot the subgraph
films = [x for x in G.nodes() if x.startswith("films")]
people = [x for x in G.nodes() if x.startswith("people")]
species = [x for x in G.nodes() if x.startswith("species")]
planets = [x for x in G.nodes() if x.startswith("planets")]
starships = [x for x in G.nodes() if x.startswith("starships")]
vehicles = [x for x in G.nodes() if x.startswith("vehicles")]
labels = {l: l.split(":")[1] for l in G.nodes()}
pos = nx.spring_layout(G)

plt.clf()
nx.draw_networkx_nodes(G, pos,
                       nodelist=films,
                       node_color='lightgrey',
                       node_size=500,
                       label="films")
nx.draw_networkx_nodes(G, pos,
                       nodelist=people,
                       node_color='indianred',
                       node_size=500,
                       label="people")
nx.draw_networkx_nodes(G, pos,
                       nodelist=species,
                       node_color='mediumturquoise',
                       node_size=500,
                       label="species")
nx.draw_networkx_nodes(G, pos,
                       nodelist=planets,
                       node_color='mediumseagreen',
                       node_size=500,
                       label="planets")
nx.draw_networkx_nodes(G, pos,
                       nodelist=starships,
                       node_color='violet',
                       node_size=500,
                       label="starships")
nx.draw_networkx_nodes(G, pos,
                       nodelist=vehicles,
                       node_color='slateblue',
                       node_size=500,
                       label="vehicles")
nx.draw_networkx_edges(G, pos, width=1.5, arrowsize=12)
nx.draw_networkx_labels(G, pos, labels, font_size=14)
plt.legend(numpoints=1)
plt.show()
