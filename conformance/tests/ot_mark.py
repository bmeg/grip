

def test_mark_select_label_filter(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V("Film:1").as_("a").\
            both("films").\
            as_("b").\
            render({"a" : "$a", "b" : "$b"}):
        count += 1
        if len(row) != 2:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "Film:1":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["b"]["label"] not in ["Vehicle", "Starship", "Species", "Planet", "Character"]:
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])

    if count != 38:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 38))

    return errors


def test_mark_select(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V("Character:1").as_("a").out().as_(
            "b").out().as_("c").render({"a": "$a", "b": "$b", "c": "$c"}):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "Character:1":
            errors.append("Incorrect vertex returned for 'a': %s" % row["a"])
        if row["a"]["data"]["height"] != 172:
            errors.append("Missing data for 'a'")
        if row["b"]["label"] not in ["Starship", "Planet", "Species", "Film"]:
            errors.append("Incorrect vertex returned for 'b': %s" % row["b"])

    if count != 64:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 64))

    return errors


def test_mark_edge_select(man):
    errors = []

    G = man.setGraph("swapi")

    count = 0
    for row in G.query().V("Film:1").as_("a").outE("planets").as_(
            "b").out().as_("c").render({"a":"$a", "b":"$b", "c":"$c"}):
        count += 1
        if len(row) != 3:
            errors.append("Incorrect number of marks returned")
        if row["a"]["gid"] != "Film:1":
            errors.append("Incorrect as selection")
        if row["b"]["label"] != "planets":
            errors.append("Incorrect as edge selection: %s" % row["b"])
        if "scene_count" not in row["b"]["data"]:
            errors.append("Data not returned")
        if row["c"]["label"] != "Planet":
            errors.append("Incorrect element returned")

    if count != 3:
        errors.append("unexpected number of rows returned. %d != %d" %
                      (count, 3))

    return errors
