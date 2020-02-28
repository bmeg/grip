from __future__ import absolute_import

"""
Queries that are designed to test the path traversal optimization
some engines (ie GRIDS) will do. Can't verify that the optimization
was applied, but does verify that the results seem correct
"""


def test_path_1(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for res in O.query().V().out().out().out():
        if res.gid not in ["01", "02", "03", "04", "05", "06", "08", "09", "40", "41", "42", "50"]:
            errors.append("Wrong vertex found at end of path: %s" % (res.gid))
        if res.label not in ["Person", "Movie", "Book"]:
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 14:
        errors.append("out-out-out Incorrect vertex count returned: %d != %d" % (count, 14))

    count = 0
    for res in O.query().V().in_().in_().in_():
        if res.gid not in ["01", "08"]:
            errors.append("Wrong vertex found at end of path: %s" % (res.gid))
        if not res.label == "Person":
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 14:
        errors.append("in-in-in Incorrect vertex count returned: %d != %d" % (count, 14))

    count = 0
    for res in O.query().V().out().out().outE():
        if not res['from'] in ["01", "02", "04", "05", "06", "08"]:
            errors.append("Wrong 'from' vertex found at end of outE path: %s" % (res['from']))
        if not res['to'] in ["01", "02", "03", "04", "05", "06", "08", "09", "40", "41", "42", "50"]:
            errors.append("Wrong 'to' vertex found at end of ourE path: %s" % (res['to']))
        if res.label not in ["likes", "knows", "friend", "parent", "enemy"]:
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 14:
        errors.append("out-out-outE Incorrect vertex count returned: %d != %d" % (count, 14))

    count = 0
    for res in O.query().V().out().out().outE().out():
        if res.gid not in ["42", "41", "02", "03", "04", "08", "09", "05", "50", "40", "06", "01"]:
            errors.append("Wrong vertex found at end of outE to out path: %s" % (res.gid))
        if res.label not in ["Movie", "Person", "Book"]:
            errors.append("Wrong label found at end of outE to out path: %s" % (res.label))
        count += 1
    if count != 14:
        errors.append("out-out-outE-out Incorrect vertex count returned: %d != %d" % (count, 14))

    return errors


def test_path_2(O, man):
    errors = []

    man.setGraph("graph1")

    count = 0
    for res in O.query().V().out().hasLabel("Person").out().out():
        if res.gid not in ["01", "02", "03", "04", "05", "06", "08", "09", "40", "41", "42", "50"]:
            errors.append("Wrong vertex found at end of hasLabel path: %s" % (res.gid))
        count += 1
    if count != 14:
        errors.append("out-hasLabel-out-out Incorrect vertex count returned: %d != %d" % (count, 3))

    return errors
