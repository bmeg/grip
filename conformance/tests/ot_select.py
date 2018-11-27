from __future__ import absolute_import

import gripql


def test_select(O):
    errors = []

    O.addVertex("1", "Reaction", {"action": "up"})
    O.addVertex("2", "Protein", {"symbol": "MDM2"})
    O.addVertex("3", "Protein", {"symbol": "TP53"})
    O.addVertex("4", "Reaction", {"action": "up"})
    O.addVertex("5", "Protein", {"symbol": "HNF4"})
    O.addVertex("6", "Protein", {"symbol": "MED1"})

    O.addEdge("1", "2", "controller", {})
    O.addEdge("1", "3", "controlled", {})
    O.addEdge("4", "5", "controller", {})
    O.addEdge("4", "6", "controlled", {})

    q = O.query().V().where(gripql.eq("_label", "Reaction")).mark("reaction")
    q = q.out("controller").where(gripql.eq("symbol", "MDM2")).select("reaction")
    q = q.out("controlled")

    found = 0
    for row in q:
        found += 1
        if row.data.symbol != "TP53":
            errors.append("Bad connection found")

    if found != 1:
        errors.append("Incorrect number of reactions found: %s != 1" % (found))
    return errors
