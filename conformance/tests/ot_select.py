from __future__ import absolute_import

import gripql


def test_simple(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.V().hasLabel("Character").as_("a").out().select("a")

    count = 0
    for row in q:
        count += 1
        if row["_label"] != "Character":
            errors.append("Wrong node label")
    if count != 52:
        errors.append("Incorrect count %d != %d" % (count, 52))
    return errors


def test_select(man):
    errors = []

    G = man.setGraph("swapi")

    q = G.V().hasLabel("Character").as_("person")
    q = q.out("homeworld").has(gripql.eq("name", "Tatooine")).select("person")
    q = q.out("species")

    found = 0
    for row in q:
        found += 1
        if row["name"] not in ["Human", "Droid"]:
            errors.append("Bad connection found: %s" % (row["name"]))

    if found != 7:
        errors.append("Incorrect number of people found: %s != 7" % (found))
    return errors
