

def test_vertex_struct(O, man):
    errors = []

    man.writeTest()

    O.addVertex("vertex1", "person", {"field1": {"test": 1, "value": False}})

    count = 0
    for i in O.query().V():
        count += 1
        p = i['data']['field1']
        if not isinstance(p, dict):
            errors.append("Dictionary data failed")
            continue
        if "test" not in p or "value" not in p:
            errors.append("missing keys in structure field")
            continue
        if p["test"] != 1 or p["value"]:
            errors.append("Incorrect values in structure")

    if count != 1:
        errors.append("Vertex struct property count failed")

    return errors


def test_edge_struct(O, man):
    errors = []

    man.writeTest()

    O.addVertex("vertex1", "person", {"field1": {"test": 1, "value": False}})
    O.addVertex("vertex2", "person", {"field1": {"test": 2, "value": True}})

    O.addEdge("vertex1", "vertex2", "friend", {"edgevals": {"weight": 3.14, "count": 15}})

    for i in O.query().V("vertex1").outE():
        if 'weight' not in i['data']['edgevals'] or i['data']['edgevals']['weight'] != 3.14:
            errors.append("out edge data not found")

    for i in O.query().V("vertex2").inE():
        if 'weight' not in i['data']['edgevals'] or i['data']['edgevals']['weight'] != 3.14:
            errors.append("in edge data not found")

    return errors


def test_nested_struct(O, man):
    errors = []

    man.writeTest()

    data = {"field1": {"nested": {"test": 1,
                                  "array": [{"value": {"entry": 1}}]}}}
    O.addVertex("vertex1", "person", data)

    count = 0
    for i in O.query().V():
        count += 1
        try:
            p = i['data']["field1"]['nested']["array"][0]["value"]["entry"]
            if p != 1:
                errors.append("Incorrect values in structure")
        except KeyError:
            errors.append(
                "Vertex not packed correctly %s != %s" %
                (data, i['data']))

    if count != 1:
        errors.append("Vertex struct property count failed")

    return errors
