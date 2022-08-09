from __future__ import absolute_import

import gripql

# test basic repeat cycle
def test_repeat(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V("Character:1").set("count", 0).as_("start").mark("a").out().increment("$start.count")
    q = q.has(gripql.lt("$start.count", 2))
    q = q.jump("a", None, True)

    count = 0
    for row in q:
        count += 1

    if count != 4:
        errors.append("cycle output count %d != %d" % (count, 4))

    #do a deeper search, to see if channels are overloaded
    q = G.query().V().set("count", 0).as_("start").mark("a").increment("$start.count")
    q = q.has(gripql.lt("$start.count", 4)).out()
    q = q.jump("a", None, True)

    count = 0
    for row in q:
        count += 1
        #print(count)
    if count != 11786:
        errors.append("cycle output count %d != %d" % (count, 11786))
    return errors

# make sure jumping forward in chain works
def test_forward(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V().jump("skip", gripql.eq( "_label", "Character" ), True).out()
    q = q.has(gripql.eq( "_label", "Character" ))
    q = q.mark("skip").path()

    count1 = 0
    count2 = 0
    for row in q:
        if not row[-1]['vertex'].startswith("Character:"):
            errors.append("Incorrect last node on path")
        if len(row) == 1:
            count1 += 1
        if len(row) == 2:
            count2 += 1
    if count1 != 18:
        errors.append("Single step count %d != %d" % (count1, 10))
    if count2 != 52:
        errors.append("Two step count %d != %d" % (count2, 10))

    return errors

# test basic repeat cycle
def test_infinite(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V("Character:1").mark("a").out()
    q = q.jump("a", None, True).limit(100)

    count = 0
    for row in q:
        count += 1

    if count != 100:
        errors.append("Loop limit returns incorrect number")

    return errors


def test_set(man):
    errors = []
    G = man.setGraph("swapi")

    q = G.query().V("Character:1").set("count", 0)
    q = q.as_("start").render("$start._data")
    for row in q:
        if row['count'] != 0:
            errors.append("Incorrect increment value")

    q = G.query().V("Character:1").set("count", 0).as_("start").out().increment("$start.count")
    q = q.render("$start._data")
    for row in q:
        if row['count'] != 1:
            errors.append("Incorrect increment value")

    q = G.query().V("Character:1").set("count", 0).as_("start").out().increment("$start.count")
    q = q.increment("$start.count").has(gripql.gt("$start.count", 1.0))
    q = q.render("$start._data")
    count = 0
    for row in q:
        count += 1
        if row['count'] != 2:
            errors.append("Incorrect increment value")
    if count != 4:
        errors.append("Incorrect number of rows returned")

    q = G.query().V("Character:1").set("count", 0).increment("count",2).as_("start").out().increment("$start.count")
    q = q.render("$start._data")
    for row in q:
        if row['count'] != 3:
            errors.append("Incorrect increment value")

    return errors
