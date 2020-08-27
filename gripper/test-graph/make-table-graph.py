#!/usr/bin/env python

import os
import sys
import json

"""
make-table-graph.py

This script take a vertex and edge file and translates it into a series of
tables that could be loaded into a varity of data systems to test the dig driver
"""


def graph2tables(vFile, eFile, ePlanFile):
    tables = {}
    vTypes = {}
    with open(vFile) as handle:
        for line in handle:
            data = json.loads(line)
            label = data['label']
            if label not in tables:
                tables[label] = []
            tables[label].append(data)
            vTypes[data['gid']] = data['label']

    eTable = []
    with open(eFile) as handle:
        for line in handle:
            data = json.loads(line)
            eTable.append(data)

    ePlan = []
    with open(ePlanFile) as handle:
        for line in handle:
            data = json.loads(line)
            ePlan.append(data)

    for plan in ePlan:
        if plan['mode'] == "edgeTable":
            o = []
            for e in eTable:
                if vTypes[e['to']] == plan['to'] and vTypes[e['from']] == plan['from'] and e['label'] == plan['label']:
                    f = e['from'].split(":")[1]
                    t = e['to'].split(":")[1]
                    d = e.get("data", {})
                    d["from"] = f
                    d["to"] = t
                    o.append( d )
        elif plan['mode'] == 'fieldToID':
            for e in eTable:
                if vTypes[e['to']] == plan['to'] and vTypes[e['from']] == plan['from'] and e['label'] == plan['label']:
                    #print("add %s to %s" % (e['to'], e['from']))
                    for v in tables[vTypes[e['from']]]:
                        if v['gid'] == e['from']:
                            dstID = e['to'].split(":")[1]
                            v['data'][ plan['field'] ] = dstID

        else:
            raise Exception("Unknown edge mode")
        tables[plan['name']] = o

    return tables


def keyUnion(a):
    o = set()
    for i in a:
        o.update(*a)
    return list(o)

if __name__ == "__main__":
    vFile = sys.argv[1]
    eFile = sys.argv[2]
    ePlanFile = sys.argv[3]
    outdir = sys.argv[4]

    tables = graph2tables(vFile, eFile, ePlanFile)
    #print(tables)

    with open(os.path.join(outdir, "table.map"), "w") as tHandle:
        for name, rows in tables.items():
            p = os.path.join(outdir, "%s.tsv" % (name))
            with open(p, "w") as handle:
                if 'data' in rows[0] and 'gid' in rows[0]:
                    headers = keyUnion( list(r['data'].keys()) for r in rows )
                    handle.write("\t".join(['id'] + headers) + "\n")
                    for row in rows:
                        id = row['gid'].split(":")[1]
                        handle.write("\t".join( [json.dumps(id)] + list( json.dumps(row['data'].get(k,"")) for k in headers ) ) + "\n")
                else:
                    headers = list(rows[0].keys())
                    handle.write("\t".join(headers) + "\n")
                    for row in rows:
                        handle.write("\t".join(list( json.dumps(row[k]) for k in headers )) + "\n")
            tHandle.write("%s\t%s.tsv\n" % (name, name))
