from __future__ import absolute_import

import gripql

def test_repeat(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V("Character:1").set("count", 0).as_("start").mark("a").out().increment("$start.count")
    #q = q.has(gripql.lt("$start.count", 5))
    q = q.jump("a", None, True)

    for row in q:
        print(row)

    return errors

def test_set(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V("Character:1").set("count", 0).as_("start").out().increment("$start.count")
    q = q.render("$start._data")
    for row in q:
        if row['count'] != 1:
            errors.append("Incorrect increment value")

    q = G.query().V("Character:1").set("count", 0).increment("count",2).as_("start").out().increment("$start.count")
    q = q.render("$start._data")
    for row in q:
        if row['count'] != 3:
            errors.append("Incorrect increment value")

    return errors
