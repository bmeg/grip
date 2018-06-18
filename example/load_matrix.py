#!/usr/bin/env python

from __future__ import print_function

import aql
import argparse
import pandas
import math


def load_matrix(args):
    conn = aql.Connection(args.server)
    O = conn.graph(args.db)

    matrix = pandas.read_csv(args.input, sep="\t", index_col=0).transpose()

    for c in matrix.columns:
        if list(O.query().V(c).count())[0]['count'] == 0:
            O.addVertex(c, "Protein")

    for name, row in matrix.iterrows():
        src = "%s:%s" % (args.data_type, name)
        print("Loading: %s" % (src))
        data = {}
        for c in matrix.columns:
            v = row[c]
            if not math.isnan(v):
                data[c] = v
        O.addVertex(name, "Sample")
        O.addVertex(src, "Data:%s" % (args.data_type), data)
        O.addEdge(name, src, "has")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--server", default="http://localhost:8201")
    parser.add_argument("--data-type", dest="data_type", default="Expression")
    parser.add_argument("--db", required=True)

    args = parser.parse_args()
    load_matrix(args)
