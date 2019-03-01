

def setupGraph(O):
    O.addVertex("vertex1", "person", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex2", "person")
    O.addVertex("vertex3", "person", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4", "person")

    O.addEdge("vertex1", "vertex2", "friend", gid="edge1")
    O.addEdge("vertex2", "vertex3", "friend", gid="edge2")
    O.addEdge("vertex2", "vertex4", "parent", gid="edge3")


def test_fields(O):
    errors = []

    setupGraph(O)

    try:
        for i in O.query().out():
            pass
        errors.append("Bad traversal query returned without exception")
    except Exception:
        pass

    return errors
