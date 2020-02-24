from __future__ import absolute_import

import gripql


def test_simple(O, man):
    errors = []

    man.setGraph("graph1")

    q = O.query().V().hasLabel("Reaction").as_("a").out().select("a")

    count = 0
    for row in q:
        count += 1
        if row.label != "Reaction":
            errors.append("Wrong node label")
    if count != 4:
        errors.append("Incorrect count %d != %d" % (count, 4))
    return errors


def test_select(O, man):
    errors = []

    man.setGraph("graph1")

    q = O.query().V().hasLabel("Reaction").as_("reaction")
    q = q.out("controller").has(gripql.eq("symbol", "MDM2")).select("reaction")
    q = q.out("controlled")

    found = 0
    for row in q:
        found += 1
        if row.data.symbol != "TP53":
            errors.append("Bad connection found")

    if found != 1:
        errors.append("Incorrect number of reactions found: %s != 1" % (found))
    return errors
