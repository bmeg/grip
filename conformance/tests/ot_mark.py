def setupGraph(O):
    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex2", "vertex4", "parent", gid="edge3")


def test_mark_select_label_filter(O):
    errors = []

    setupGraph(O)

    count = 0
    for row in O.query().V("vertex2").mark("a").both("friend").mark(
            "b").select(["a", "b"]):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "vertex2":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["b"]["gid"] not in ["vertex1", "vertex3"]:
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])
        if row["b"]["gid"] == "vertex1":
            if row["b"]["data"] != {"field1": "value1", "field2": "value2"}:
                errors.append("Missing data for 'b': %s")
        if row["b"]["gid"] == "vertex3":
            if row["b"]["data"] != {"field1": "value3", "field2": "value4"}:
                errors.append("Missing data for 'b'")

    if count != 2:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 2))

    return errors


def test_mark_select(O):
    errors = []

    setupGraph(O)

    count = 0
    for row in O.query().V("vertex1").mark("a").out().mark(
            "b").out().mark("c").select(["a", "b", "c"]):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "vertex1":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["a"]["data"] != {"field1": "value1", "field2": "value2"}:
            errors.append("Missing data for 'a'")
        if row["b"]["gid"] != "vertex2":
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])
        if row["c"]["gid"] not in ["vertex3", "vertex4"]:
            errors.append("Incorrect vertex returned for 'c': %s" % row["c"])
        if row["c"]["gid"] == "vertex3":
            if row["c"]["data"] != {"field1": "value3", "field2": "value4"}:
                errors.append("Missing data for 'c'")

    if count != 2:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 2))

    return errors


def test_mark_edge_select(O):
    errors = []

    setupGraph(O)

    count = 0
    for row in O.query().V("vertex1").mark("a").outEdge().mark(
            "b").out().mark("c").select(["a", "b", "c"]):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "vertex1":
            errors.append("Incorrect as selection")
        if row["b"]["gid"] != "edge1":
            errors.append("Incorrect as edge selection: %s" % row["b"])
        if row["c"]["gid"] != "vertex2":
            errors.append("Incorrect as selection")

    if count != 1:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 1))

    return errors


def test_mark_select_fields(O):
    errors = []

    setupGraph(O)

    count = 0
    for row in O.query().V("vertex1").mark("a").out().\
            mark("b").out().mark("c").\
            fields(["$a._gid", "$b._gid", "$c._gid", "$c.field1"]).\
            select(["a", "b", "c"]):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "vertex1":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["a"]["data"] != {}:
            errors.append("Incorrect data for 'a'")
        if row["b"]["gid"] != "vertex2":
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])
        if row["c"]["gid"] not in ["vertex3", "vertex4"]:
            errors.append("Incorrect vertex returned for 'c': %s" % row["c"])
        if row["c"]["gid"] == "vertex3":
            if row["c"]["data"] != {"field1": "value3"}:
                errors.append("Incorrect data for 'c'")

    if count != 2:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 2))

    return errors
