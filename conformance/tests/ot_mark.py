

def test_mark_select_label_filter(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for row in O.query().V("02").as_("a").\
            both("parent").\
            as_("b").\
            select(["a", "b"]):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "02":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["b"]["gid"] not in ["05"]:
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])

    if count != 1:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 1))

    return errors


def test_mark_select(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for row in O.query().V("01").as_("a").out().as_(
            "b").out().as_("c").select(["a", "b", "c"]):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "01":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["a"]["data"] != {"age": 29, "name": "marko"}:
            errors.append("Missing data for 'a'")
        if row["b"]["gid"] not in ["02", "03", "04", "05", "06", "08"]:
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])
        if row["c"]["gid"] not in ["01", "03", "05", "06", "08", "09", "40", "50"]:
            errors.append("Incorrect vertex returned for 'c': %s" % row["c"])
        else:
            if "name" not in row["c"]["data"]:
                errors.append("Missing data for 'c'")

    if count != 7:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 7))

    return errors


def test_mark_edge_select(O, man):
    errors = []

    man.setGraph("swapi")

    count = 0
    for row in O.query().V("08").as_("a").outE().as_(
            "b").out().as_("c").select(["a", "b", "c"]):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "08":
            errors.append("Incorrect as selection")
        if row["b"]["gid"] != "edge08-01":
            errors.append("Incorrect as edge selection: %s" % row["b"])
        if row["c"]["gid"] != "01":
            errors.append("Incorrect as selection")

    if count != 1:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 1))

    return errors
