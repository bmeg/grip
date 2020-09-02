from __future__ import absolute_import

"""
Queries that are designed to test the path traversal optimization
some engines (ie GRIDS) will do. Can't verify that the optimization
was applied, but does verify that the results seem correct
"""


def test_path_1(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for res in G.query().V("Film:1").out().out().out():
        count += 1
    if count != 1814:
        errors.append("out-out-out Incorrect vertex count returned: %d != %d" % (count, 1814))

    count = 0
    for res in G.query().V("Film:1").in_().in_().in_():
        count += 1
    if count != 1814:
        errors.append("in-in-in Incorrect vertex count returned: %d != %d" % (count, 1814))

    count = 0
    for res in G.query().V("Film:1").out().out().outE():
        if res.label not in ["vehicles", "species", "planets", "characters", "enemy", "starships", "films", "homeworld", "people", "pilots", "residents"]:
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 1814:
        errors.append("out-out-outE Incorrect vertex count returned: %d != %d" % (count, 1814))

    count = 0
    for res in G.query().V("Film:1").out().out().outE().out():
        count += 1
    if count != 1814:
        errors.append("out-out-outE-out Incorrect vertex count returned: %d != %d" % (count, 1814))

    return errors


def test_path_2(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for res in G.query().V().out().hasLabel("Starship").out().out():
        count += 1
    if count != 666:
        errors.append("out-hasLabel-out-out Incorrect vertex count returned: %d != %d" % (count, 666))

    return errors
