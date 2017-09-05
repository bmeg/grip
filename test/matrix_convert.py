#!/usr/bin/env python

import json
import argparse
import pandas



def parse_matrix(path, prefix=""):
    matrix = pandas.read_csv(path, sep="\t", index_col=0 )
    edges = []
    for c in matrix.columns:
        bundle = {}
        col = matrix.loc[:,c]
        for g in matrix.index:
            name = prefix + g.split("|")[0]
            bundle[name] = {"v" : col[g] }
        o = {
            "from" : c,
            "label" : "expression",
            "bundle" : bundle
        }
        edges.append(o)
    return edges

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("-p", "--prefix", default="")
    parser.add_argument("-o", "--out", default="matrix_graph")

    args = parser.parse_args()

    edges = parse_matrix(args.input, args.prefix)

    
    for i in edges:
        print json.dumps(i)
