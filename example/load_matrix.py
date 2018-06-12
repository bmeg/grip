#!/usr/bin/env python

from __future__ import print_function

import aql
import argparse
import pandas
import math


def load_matrix(args):
    conn = aql.Connection(args.server)
    if args.db not in list(conn.listGraphs()):
        conn.addGraph(args.db)
    O = conn.graph(args.db)

    if args.columns is not None:
        matrix = pandas.read_csv(args.input, sep=args.sep, index_col=0, header=None, names=args.columns)
    else:
        matrix = pandas.read_csv(args.input, sep=args.sep, index_col=0)
    if args.transpose:
        matrix = matrix.transpose()

    if args.connect:
        #every row x col creates an edge with the weight value
        for c in matrix.columns:
            cname = "%s%s" % (args.col_prefix, c)
            if list(O.query().V(c).count())[0]['count'] == 0:
                if args.debug:
                    print("AddVertex %s %s" % (c, args.col_label))
                else:
                    O.addVertex(c, args.col_label)
        for r in matrix.index:
            rname = "%s%s" % (args.row_prefix, r)
            if list(O.query().V(r).count())[0]['count'] == 0:
                if args.debug:
                    print("AddVertex %s %s" % (r, args.row_label))
                else:
                    O.addVertex(r, args.row_label)

        for name, row in matrix.iterrows():
            rname = "%s%s" % (args.row_prefix, name)
            print("Loading: %s" % (rname))
            b = O.bulkAdd()
            for c in matrix.columns:
                cname = "%s%s" % (args.col_prefix, c)
                v = row[c]
                if not math.isnan(v):
                    if args.debug:
                        print("AddEdge: %s %s %s %s" % (rname,cname,args.edge_label,{args.edge_prop:v}) )
                    else:
                        b.addEdge(rname, cname, args.edge_label, {args.edge_prop:v})
            b.execute()
    else:
        for name, row in matrix.iterrows():
            rname = "%s%s" % (args.row_prefix, name)
            print("Loading: %s" % (rname))
            data = {}
            for c in matrix.columns:
                v = row[c]
                if not isinstance(v,float) or not math.isnan(v):
                    data[c] = v
            if args.debug:
                print("Add Vertex %s %s %s" % (rname, args.row_label, data))
            else:
                O.addVertex(rname, args.row_label, data)
            for dst, edge in args.edge:
                if args.debug:
                    print("Add Edge %s %s" % (dst.format(**data), edge))
                else:
                    O.addEdge(rname, dst.format(**data), edge)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("input")
    parser.add_argument("--sep", default="\t")
    parser.add_argument("--server", default="http://localhost:8201")
    parser.add_argument("--row-label", dest="row_label", default="Row")
    parser.add_argument("--row-prefix", default="")
    parser.add_argument("-t", "--transpose", action="store_true", default=False)
    parser.add_argument("--connect", action="store_true", default=False)
    parser.add_argument("--col-label", dest="col_label", default="Col")
    parser.add_argument("--col-prefix", default="")
    parser.add_argument("--columns", default=None, nargs="*")

    parser.add_argument("--edge-label", dest="edge_label", default="weight")
    parser.add_argument("--edge-prop", dest="edge_prop", default="w")
    parser.add_argument("-d", dest="debug", action="store_true", default=False)

    parser.add_argument("-e", "--edge", action="append", default=[], nargs=2)

    args = parser.parse_args()
    load_matrix(args)
