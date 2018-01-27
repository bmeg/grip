#!/usr/bin/env python

import aql
import json
import argparse
import pandas
import math


def load_matrix(args):
    conn = aql.Connection(args.server)
    O = conn.graph(args.db)

    matrix = pandas.read_csv(args.input, sep="\t", index_col=0)

    for name, row in matrix.iterrows():
        data = {}
        for k, v in row.iteritems():
            if not isinstance(v, float) or not math.isnan(v):
                data[k] = v
        O.addVertex(name, "Sample", data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--server", default="http://localhost:8000")
    parser.add_argument("--db", default="test-data")
    parser.add_argument("-p", "--prefix", default="")
    parser.add_argument("-d", dest="debug", action="store_true", default=False )

    args = parser.parse_args()
    load_matrix(args)
