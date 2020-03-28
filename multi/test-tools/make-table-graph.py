#!/usr/bin/env python

import os
import sys
import json


if __name__ == "__main__":
    vFile = sys.argv[1]
    eFile = sys.argv[2]
    outdir = sys.argv[3]

    outFiles = {}
    outPaths = {}
    headers = {}
    with open(vFile) as handle:
        for line in handle:
            data = json.loads(line)
            label = data['label']
            if label not in outFiles:
                p = os.path.join(outdir, "%s.tsv" % (label))
                outFiles[label] = open(p, "w")
                headers[label] = list(data['data'].keys())
                outFiles[label].write("\t".join(['id'] + headers[label]) + "\n")
                outPaths[label] = p
            id = data['gid'].split(":")[1]
            outFiles[label].write("\t".join( [id] + list( str(data['data'][k]) for k in headers[label] ) ) + "\n")

        for h in outFiles.values():
            h.close()

    with open(os.path.join(outdir, "table.map"), "w") as handle:
        for k, v in outPaths.items():
            handle.write("%s\t%s\n" % (k,os.path.basename(v)))
