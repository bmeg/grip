#!/usr/bin/env python

from __future__ import print_function

import aql
import argparse


def load_sif(args):
    conn = aql.Connection(args.server)
    O = conn.graph(args.db)
    proteins = set()
    with open(args.input) as handle:
        rows = []
        for line in handle:
            row = line.rstrip().split("\t")
            if not (row[0].startswith("CHEBI:") or row[2].startswith("CHEBI:")):
                rows.append(row)
                proteins.add(row[0])
                proteins.add(row[2])

    print("Loading Proteins")
    for i in proteins:
        O.addVertex(i, "Protein", {})

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    i = 0
    for chunk in chunks(rows, 10000):
        b = O.bulkAdd()
        for row in chunk:
            b.addEdge(row[0], row[2], row[1], {})
        b.execute()
        i += len(chunk)
        print("Loaded %s edges" % i)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--server", default="http://localhost:8201")
    parser.add_argument("--db", required=True)

    args = parser.parse_args()
    load_sif(args)
