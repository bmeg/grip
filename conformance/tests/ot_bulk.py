def test_bulkload(man):
    errors = []

    G = man.writeTest()

    bulk = G.bulkAdd()

    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})

    bulk.addEdge("1", "3", "created", {"weight": 0.4})
    bulk.addEdge("1", "2", "knows", {"weight": 0.5})
    bulk.addEdge("1", "4", "knows", {"weight": 1.0})
    bulk.addEdge("4", "3", "created", {"weight": 0.4})
    bulk.addEdge("6", "3", "created", {"weight": 0.2})
    bulk.addEdge("4", "5", "created", {"weight": 1.0})

    err = bulk.execute()

    if err.get("errorCount", 0) != 0:
        print(err)
        errors.append("Bulk insertion error")

    res = G.query().V().count().execute()[0]
    if res["count"] != 6:
        errors.append(
            "Bulk Add wrong number of vertices: %s != %s" %
            (res["count"], 6))

    res = G.query().E().count().execute()[0]
    if res["count"] != 6:
        errors.append(
            "Bulk Add wrong number of edges: %s != %s" %
            (res["count"], 6))

    return errors


def test_bulkload_validate(man):
    errors = []

    G = man.writeTest()

    bulk = G.bulkAdd()

    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})

    bulk.addEdge("1", None, "created", {"weight": 0.4})
    bulk.addEdge("1", "2", "knows", {"weight": 0.5})
    bulk.addEdge("1", "4", "knows", {"weight": 1.0})
    bulk.addEdge("4", "3", "created", {"weight": 0.4})
    bulk.addEdge("6", "3", "created", {"weight": 0.2})
    bulk.addEdge("4", "5", None, {"weight": 1.0})

    err = bulk.execute()
    if err["errorCount"] == 0:
        errors.append("Validation error not detected")

    return errors


def test_bulk_delete(man):
    errors = []
    G = man.writeTest()

    G.addVertex("vertex1", "Person", {"name": "marko", "age": "29"})
    G.addVertex("vertex2", "Person", {"name": "vadas", "age": "27"})
    G.addVertex("vertex3", "Software", {"name": "lop", "lang": "java"})
    G.addVertex("vertex4", "Person", {"name": "josh", "age": "32"})
    G.addVertex("vertex5", "Software", {"name": "ripple", "lang": "java"})
    G.addVertex("vertex6", "Person", {"name": "peter", "age": "35"})

    G.addEdge("vertex1", "vertex3", "created", {"weight": 0.4}, gid="edge1")
    G.addEdge("vertex1", "vertex2", "knows", {"weight": 0.5}, gid="edge2")
    G.addEdge("vertex1", "vertex4", "knows", {"weight": 1.0}, gid="edge3")
    G.addEdge("vertex4", "vertex3", "created", {"weight": 0.4}, gid="edge4")
    G.addEdge("vertex6", "vertex3", "created", {"weight": 0.2}, gid="edge5")
    G.addEdge("vertex3", "vertex5", "created", {"weight": 1.0}, gid="edge6")
    G.addEdge("vertex6", "vertex5", "created", {"weight": 1.0}, gid="edge7")
    G.addEdge("vertex4", "vertex5", "created", {"weight": 0.4}, gid="edge8")
    G.addEdge("vertex4", "vertex6", "created", {"weight": 0.4}, gid="edge9")

    G.delete(vertices=["vertex1", "vertex2",
                       "vertex3"],
             edges=[])

    Ecount = G.query().E().count().execute()[0]["count"]
    Vcount = G.query().V().count().execute()[0]["count"]
    if Ecount != 3:
        errors.append(f"Wrong number of edges {Ecount} != 3")
    if Vcount != 3:
        errors.append(f"Wrong number of vertices {Vcount} != 3")

    G.delete(vertices=[], edges=["edge7"])
    Ecount = G.query().E().count().execute()[0]["count"]
    Vcount = G.query().V().count().execute()[0]["count"]
    if Ecount != 2:
        errors.append(f"Wrong number of edges {Ecount} != 2")
    if Vcount != 3:
        errors.append(f"Wrong number of vertices {Vcount} != 3")


    G.delete(vertices=["vertex5", "vertex6"], edges=["edge9"])
    Ecount = G.query().E().count().execute()[0]["count"]
    Vcount = G.query().V().count().execute()[0]["count"]
    if Ecount != 0:
        errors.append(f"Wrong number of edges {Ecount} != 0")
    if Vcount != 1:
        errors.append(f"Wrong number of vertices {Vcount} != 1")

    return errors
