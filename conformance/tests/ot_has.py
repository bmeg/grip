def test_has(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().has("occupation", "jedi").execute():
        count += 1
        if i['vertex']['gid'] not in ["vertex2", "vertex5"]:
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 2:
        errors.append(
            "Fail: O.query().V().has('occupation', 'jedi') %s != %s" %
            (count, 2))

    return errors


def test_has_label(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().hasLabel("person").execute():
        count += 1
        if i['vertex']['label'] != "person":
            errors.append("Wrong vertex label %s" % (i['vertex']['label']))
    if count != 3:
        errors.append(
            "Fail: O.query().V().hasLabel('person') %s != %s" %
            (count, 3))

    return errors


def test_has_id(O):
    errors = []

    O.addVertex("vertex1", "person", {"name": "han", "occupation": "smuggler"})
    O.addVertex("vertex2", "person", {"name": "luke", "occupation": "jedi"})
    O.addVertex("vertex3", "robot", {"name": "r2-d2"})
    O.addVertex("vertex4", "robot", {"name": "c-3po"})
    O.addVertex("vertex5", "person", {"name": "obi-wan", "occupation": "jedi"})

    O.addEdge("vertex1", "vertex2", "friend", id="edge1")
    O.addEdge("vertex2", "vertex3", "owner", id="edge2")
    O.addEdge("vertex2", "vertex4", "friend", id="edge3")

    count = 0
    for i in O.query().V().hasId("vertex3").execute():
        count += 1
        if i['vertex']['gid'] != "vertex3":
            errors.append("Wrong vertex returned %s" % (i['vertex']))
    if count != 1:
        errors.append(
            "Fail: O.query().V().hasId('vertex3') %s != %s" %
            (count, 1))

    return errors
