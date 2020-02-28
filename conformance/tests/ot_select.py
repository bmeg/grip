from __future__ import absolute_import

import gripql


def test_simple(O, man):
    errors = []

    man.setGraph("swapi")

    q = O.query().V().hasLabel("Person").as_("a").out().select("a")

    count = 0
    for row in q:
        count += 1
        if row.label != "Person":
            errors.append("Wrong node label")
    if count != 14:
        errors.append("Incorrect count %d != %d" % (count, 14))
    return errors


def test_select(O, man):
    errors = []

    man.setGraph("swapi")

    q = O.query().V().hasLabel("Person").as_("person")
    q = q.out("knows").has(gripql.eq("name", "alex")).select("person")
    q = q.out("likes")

    found = 0
    for row in q:
        print(row)
        found += 1
        if row.data.name != "The Joy of Cooking":
            errors.append("Bad connection found")

    if found != 1:
        errors.append("Incorrect number of people found: %s != 1" % (found))
    return errors
