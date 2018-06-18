#!/usr/bin/env python
from __future__ import print_function

import aql
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
    parser.add_argument("--server", default="http://localhost:8201")
    parser.add_argument("--db", required=True)

    args = parser.parse_args()
    load_matrix(args)
