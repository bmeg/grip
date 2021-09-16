
from __future__ import absolute_import


def test_path_out_out_out(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for res in G.query().V("Film:1").out().out().out().path():
        if res[0]['vertex'] != "Film:1":
            errors.append("Wrong first step %s != %s" % (res[0]['vertex'], "Film:1"))
        count += 1
    if count != 1814:
        errors.append("Incorrect count returned %d != %d" % (count, 1814))
    return errors

def test_path_in_in(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for res in G.query().V("Film:1").in_().in_().path():
        if res[0]['vertex'] != "Film:1":
            errors.append("Wrong first step %s != %s" % (res[0]['vertex'], "Film:1"))
        count += 1
    if count != 106:
        errors.append("Incorrect count returned %d != %d" % (count, 1814))
    return errors

def test_path_outE_out_select(man):
    errors = []
    G = man.setGraph("swapi")
    count = 0
    for res in G.query().V("Film:1").as_("a").outE().as_("b").out().select("b").path():
        count += 1
        if len(res) != 4:
            errors.append("Wrong path length %d != %d" % (4, len(res)))
        else:
            if res[1] != res[3]:
                errors.append("path select failed")
    if count == 0:
        errors.append("No results Received")
    return errors

def test_path_out_out_select(man):
    errors = []
    G = man.setGraph("swapi")
    count = 0
    for res in G.query().V("Film:1").as_("a").out().as_("b").out().select("a").path():
        if len(res) != 4:
            errors.append("Wrong path length %d != %d" % (4, len(res)))
        else:
            if res[0] != res[3]:
                errors.append("path select failed")
    return errors
