from __future__ import absolute_import


def test_render(man):
    errors = []

    G = man.setGraph("swapi")

    query = G.query().V().hasLabel("Character").render(
        {
            "Name": "name",
            "Age": "age"
        }
    )
    count = 0
    for row in query:
        count += 1
        if 'Age' not in row or "Name" not in row:
            errors.append("Missing fields")
    if count != 18:
        errors.append("Incorrect number of rows returned")
    query = G.query().V().hasLabel("Character").render(
        {
            "Name": "name",
            "NonExistent": "non-existent"
        }
    )
    count = 0
    for row in query:
        count += 1
        if 'NonExistent' not in row or "Name" not in row:
            errors.append("Missing fields")
    if count != 18:
        errors.append("Incorrect number of rows returned")

    query = G.query().V().hasLabel("Character").render(["name", "age"])
    for row in query:
        count += 1
        if not isinstance(row, list):
            errors.append("unexpected output format")
        if len(row) != 2:
            errors.append("Missing fields")

    query = G.query().V().hasLabel("Character").render(["name", "non-existent"])
    for row in query:
        if not isinstance(row, list):
            errors.append("unexpected output format")
        if len(row) != 2:
            errors.append("Missing fields")

    return errors


def test_render_mark(man):
    """
    test_render_mark check if various mark symbols are recalled correctly
    """
    errors = []

    G = man.setGraph("swapi")

    query = G.query().V().hasLabel("Character").as_("char").out("starships").render(["$char.name", "$._gid", "$"])
    for row in query:
        if not isinstance(row[0], str):
            errors.append("incorrect return type: %s", row[0])
        if '_gid' not in row[2]:
            errors.append("incorrect return type: %s", row[2])
        if '_label' not in row[2]:
            errors.append("incorrect return type: %s", row[2])
        #print(row)

    return errors