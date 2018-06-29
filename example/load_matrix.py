#!/usr/bin/env python

from __future__ import print_function

import aql
import re
import argparse
import pandas
import math
import json

def load_matrix(args):
    conn = aql.Connection(args.server)
    if args.db not in list(conn.listGraphs()):
        conn.addGraph(args.db)
    O = conn.graph(args.db)

    if args.columns is not None:
        matrix = pandas.read_csv(args.input, sep=args.sep, index_col=args.index_col, header=None, names=args.columns, skiprows=args.skiprows)
    else:
        matrix = pandas.read_csv(args.input, sep=args.sep, index_col=args.index_col, skiprows=args.skiprows)
    if args.transpose:
        matrix = matrix.transpose()

    if args.dump is not None:
        dump_vertex_file = open(args.dump + ".vertex", "w")
        dump_edge_file = open(args.dump + ".edge", "w")
        def dump_vertex(gid, label, data):
            print("Add Vertex: %s" % (gid))
            dump_vertex_file.write(json.dumps({"gid":gid, "label":label, "data":data}) + "\n")
        def dump_edge(src, dst, label, data):
            print("Add Edge: %s %s" % (src, dst))
            dump_edge_file.write(json.dumps({"from":src, "to":dst, "label": label, "data":data})+"\n")

    if args.connect:
        if not args.no_vertex:
            #every row x col creates an edge with the weight value
            for c in matrix.columns:
                cname = "%s%s" % (args.col_prefix, c)
                if list(O.query().V(c).count())[0]['count'] == 0:
                    if args.dump is not None:
                        dump_vertex(c, args.col_label, {})
                    else:
                        O.addVertex(c, args.col_label)
            for r in matrix.index:
                rname = "%s%s" % (args.row_prefix, r)
                if list(O.query().V(r).count())[0]['count'] == 0:
                    if args.dump:
                        dump_vertex(r, args.row_label, {})
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
                    if args.dump:
                        dump_edge(rname, cname, args.edge_label, {args.edge_prop:v})
                    else:
                        b.addEdge(rname, cname, args.edge_label, {args.edge_prop:v})
            b.execute()
    else:
        if args.col_regex is not None:
            col_map = {}
            for col in matrix.columns:
                new_col = re.sub(args.col_regex[0], args.col_regex[1], col)
                col_map[col] = new_col
            matrix = matrix.rename(columns=col_map)
        for name, row in matrix.iterrows():
            rname = "%s%s" % (args.row_prefix, name)
            print("Loading: %s" % (rname))
            data = {}
            for c in matrix.columns:
                v = row[c]
                if args.column_include is None or c in args.column_include:
                    if c not in args.column_exclude:
                        if not isinstance(v,float) or not math.isnan(v):
                            data[c] = v
            for col, reg, rep in args.regex:
                data[col] = re.sub(reg, rep, data[col])
            if not args.no_vertex and rname not in args.exclude:
                if args.dump:
                    dump_vertex(rname, args.row_label, data)
                else:
                    O.addVertex(rname, args.row_label, data)
            data["_rowname"] = name
            data["_gid"] = rname
            for dst, edge in args.edge:
                try:
                    dstFmt = dst.format(**data)
                except KeyError:
                    print("Formatting Error")
                    dstFmt = None
                if dstFmt is not None:
                    if args.dump:
                        dump_edge(rname, dstFmt, edge, {})
                    else:
                        O.addEdge(rname, dstFmt, edge)
            for dst, label in args.dst_vertex:
                try:
                    dstFmt = dst.format(**data)
                except KeyError:
                    dstFmt = None
                if dstFmt is not None:
                    if list(O.query().V(dstFmt).count())[0]['count'] == 0:
                        if args.dump:
                            dump_vertex(dstFmt, label, {})
                        else:
                            O.addVertex(dstFmt, label, {})
    if args.dump is not None:
        dump_vertex_file.close()
        dump_edge_file.close()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db", help="Destination Graph")
    parser.add_argument("input", help="Input File")
    parser.add_argument("--sep", default="\t", help="TSV delimiter")
    parser.add_argument("--server", default="http://localhost:8201", help="Server Address")
    parser.add_argument("--row-label", dest="row_label", default="Row", help="Vertex Label used when loading rows")
    parser.add_argument("--row-prefix", default="", help="Prefix added to row vertex gid")
    parser.add_argument("-t", "--transpose", action="store_true", default=False, help="Transpose matrix")
    parser.add_argument("--index-col", default=0, type=int, help="Column number to use as index (and gid for vertex load)")
    parser.add_argument("--skiprows", default=None, type=int, help="Skip rows at top of file")
    parser.add_argument("--connect", action="store_true", default=False, help="Switch to 'fully connected mode' and load matrix cell values on edges between row and column names")
    parser.add_argument("--col-label", dest="col_label", default="Col", help="Column vertex label in 'connect' mode")
    parser.add_argument("--col-prefix", default="", help="Prefix added to col vertex gid in 'connect' mode")
    parser.add_argument("--edge-label", dest="edge_label", default="weight", help="Edge label for edges in 'connect' mode")
    parser.add_argument("--edge-prop", dest="edge_prop", default="w", help="Property name for storing value when in 'connect' mode")

    parser.add_argument("--columns", default=None, nargs="*", help="Rename columns in TSV")
    parser.add_argument("--column-include", default=None, action="append", help="List subset of columns to use from TSV")
    parser.add_argument("--column-exclude", default=[], nargs="*", help="List of columns to remove from TSV")

    parser.add_argument("--no-vertex", action="store_true", default=False, help="Do not load row as vertex")
    parser.add_argument("-e", "--edge", action="append", default=[], nargs=2, help="Create an edge the connected the current row vertex args: <dst> <edgeType>")
    parser.add_argument("--dst-vertex", action="append", default=[], nargs=2, help="Create a destination vertex, args: <dstVertex> <vertexLabel>")
    parser.add_argument("-x", "--exclude", action="append", default=[], help="Exclude row id")

    parser.add_argument("--regex", action="append", default=[], nargs=3, help="Run regex replace command on a specific column: <column_name> <regex> <replace>")
    parser.add_argument("--col-regex", default=None, nargs=2, help="Run regex replace command on column names: <regex> <replace>")

    parser.add_argument("-d", dest="dump", default=None, help="Dump to file and Print actions")

    args = parser.parse_args()
    if args.index_col < 0:
        args.index_col = None
    load_matrix(args)
