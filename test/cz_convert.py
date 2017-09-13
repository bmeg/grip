#!/usr/bin/env python

import argparse
import json


def parse_cz(handle, name_prefix=""):
    name_map = {}
    nodes = {}
    edges = {}
    m = json.loads(handle.read())
    for i in m:
        if 'nodes' in i:
            for n in i['nodes']:
                node_name = name_prefix + n['n']
                nodes[n['@id']] = { 'gid' : node_name, 'properties' : {} }
                name_map[n['@id']] = node_name
        elif 'nodeAttributes' in i:
            for n in i['nodeAttributes']:
                if n['n'] == "type":
                    nodes[n['po']]['label'] = n['v']
                else:
                    nodes[n['po']]['properties'][n['n']] = n['v']

    for i in m:
        if 'edges' in i:
            #print i['edges']
            for n in i['edges']:
                edges[n['@id']] = {
                    "label" : n["i"],
                    "from"  : name_map[n['s']],
                    "to"    : name_map[n['t']],
                    'properties' : {}
                }
        elif 'edgeAttributes' in i:
            #print i['edgeAttributes']
            for n in i['edgeAttributes']:
                edges[n['po']]['properties'][n['n']] = n['v']
        else:
            pass
            #print i.keys()

    return nodes, edges

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", default="")
    parser.add_argument("-o", "--out", default="graph")
    parser.add_argument("input")

    args = parser.parse_args()

    with open(args.input) as handle:
        nodes, edges = parse_cz(handle, args.prefix)
    with open(args.out + ".vertices", "w") as handle:
        for k, v in nodes.items():
            handle.write(json.dumps(v) + "\n")
    with open(args.out + ".edges", "w") as handle:
        for k, v in edges.items():
            handle.write(json.dumps(v) + "\n")
