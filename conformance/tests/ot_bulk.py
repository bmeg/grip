

def test_bulkload(O):
    errors = []

    bulk = O.bulkAdd()

    bulk.addVertex("1", "Person", {"name":"marko", "age":"29"})
    bulk.addVertex("2", "Person", {"name":"vadas", "age":"27"})
    bulk.addVertex("3", "Software", {"name":"lop", "lang":"java"})
    bulk.addVertex("4", "Person", {"name":"josh", "age":"32"})
    bulk.addVertex("5", "Software", {"name":"ripple", "lang":"java"})
    bulk.addVertex("6", "Person", {"name":"peter", "age":"35"})

    bulk.addEdge("1", "3", "created", {"weight":0.4})
    bulk.addEdge("1", "2", "knows", {"weight":0.5})
    bulk.addEdge("1", "4", "knows", {"weight":1.0})
    bulk.addEdge("4", "3", "created", {"weight":0.4})
    bulk.addEdge("6", "3", "created", {"weight":0.2})
    bulk.addEdge("4", "5", "created", {"weight":1.0})

    bulk.commit()

    res = list(O.query().V().count())[0]
    if res["data"] != 6:
        errors.append("Bulk Add wrong number of vertices: %s != %s" % (res["data"], 6))


    res = list(O.query().E().count())[0]
    if res["data"] != 6:
        errors.append("Bulk Add wrong number of edges: %s != %s" % (res["data"], 6))

    return errors
