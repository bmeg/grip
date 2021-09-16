

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
    print(err)
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
    print(err)
    return errors
