#!/usr/bin/env python

import aql
import json
import argparse
import pandas
import math


def load_matrix(args):
    conn = aql.Connection(args.server)
    O = conn.graph(args.db)

    matrix = pandas.read_csv(args.input, sep="\t", index_col=0).transpose()

    for c in matrix.columns:
        if O.query().V(c).count().first()['data'] == 0:
            if args.debug:
                print "AddVertex", c
            else:
                O.addVertex(c, "Protein")

    for name, row in matrix.iterrows():
        src = "%s:%s" % (args.data_type, name)
        print "Loading: %s" % (src)
        data = {}
        for c in matrix.columns:
            v = row[c]
            if not math.isnan(v):
                data[c] = v
        if args.debug:
            print "Add Vertex", name
        else:
            O.addVertex(name, "Sample")
        if args.debug:
            print "AddVertex", "Data:%s" % (args.data_type)
        else:
            O.addVertex(src, "Data:%s" % (args.data_type), data)
        if args.debug:
            print "AddEdge", name
        else:
            O.addEdge(name, src, "has")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--server", default="http://localhost:8201")
    parser.add_argument("--data-type", dest="data_type", default="expression")
    parser.add_argument("--db", default="test-data")
    parser.add_argument("-p", "--prefix", default="")
    parser.add_argument("-d", dest="debug", action="store_true", default=False )

    args = parser.parse_args()

    edges = load_matrix(args)
