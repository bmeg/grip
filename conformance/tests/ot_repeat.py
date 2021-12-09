from __future__ import absolute_import

import gripql

def test_repeat(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V().set("count", 0).as("start").mark("a").out().increment("$start.count")
    q = q.has(gripql.lt("$start.count", 5)).jump(nil, "a", True)

    for row in q:
        print(row)

    return errors
