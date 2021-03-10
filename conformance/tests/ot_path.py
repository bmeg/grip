
from __future__ import absolute_import


def test_path_1(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for res in G.query().V("Film:1").out().out().out().path():
        if res[0]['vertex'] != "Film:1":
            errors.append("Wrong first step %s != %s" % (res[0], "Film:1"))
        count += 1
    if count != 1814:
        errors.append("Incorrect count returned %d != %d" % (count, 1814))
    return errors
