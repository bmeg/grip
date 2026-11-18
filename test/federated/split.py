"""Split a graph (JSON lines vertices and edges) into one file pair per ID prefix.

Edges are stored with their source vertex, so edges between prefixes cross
resource nodes. Prints the prefixes found, one per line.

usage: split.py <vertices file> <edges file> <output dir>
"""
import json
import os
import sys


def prefix_of(gid):
    return gid.split(":", 1)[0] + ":"


def main(vertices, edges, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    handles = {}

    def out(prefix, kind):
        key = (prefix, kind)
        if key not in handles:
            name = prefix.rstrip(":") + "." + kind
            handles[key] = open(os.path.join(out_dir, name), "w")
        return handles[key]

    prefixes = set()
    with open(vertices) as handle:
        for line in handle:
            if not line.strip():
                continue
            p = prefix_of(json.loads(line)["_id"])
            prefixes.add(p)
            out(p, "vertices").write(line)
    with open(edges) as handle:
        for line in handle:
            if not line.strip():
                continue
            p = prefix_of(json.loads(line)["_from"])
            prefixes.add(p)
            out(p, "edges").write(line)
    for h in handles.values():
        h.close()
    for p in sorted(prefixes):
        # make sure both files exist for every prefix
        for kind in ("vertices", "edges"):
            path = os.path.join(out_dir, p.rstrip(":") + "." + kind)
            if not os.path.exists(path):
                open(path, "w").close()
        print(p)


if __name__ == "__main__":
    main(*sys.argv[1:4])
