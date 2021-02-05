

def test_index(man):
    errors = []

    G = man.writeTest()

    G.addIndex("Person", "name")

    G.addVertex("1", "Person", {"name": "marko", "age": "29"})
    G.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    G.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    G.addVertex("4", "Person", {"name": "josh", "age": "32"})
    G.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    G.addVertex("6", "Person", {"name": "peter", "age": "35"})
    G.addVertex("7", "Person", {"name": "marko", "age": "35"})

    G.addEdge("1", "3", "created", {"weight": 0.4})
    G.addEdge("1", "2", "knows", {"weight": 0.5})
    G.addEdge("1", "4", "knows", {"weight": 1.0})
    G.addEdge("4", "3", "created", {"weight": 0.4})
    G.addEdge("6", "3", "created", {"weight": 0.2})
    G.addEdge("4", "5", "created", {"weight": 1.0})

    resp = G.listIndices()
    found = False
    for i in resp:
        if i["field"] == "name" and i["label"] == "Person":
            found = True
    if not found:
        errors.append("Expected index not found")

    return errors
