

def test_index(O):
    errors = []

    O.addIndex("name")

    O.addVertex("1", "Person", {"name": "marko", "age": "29"})
    O.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    O.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    O.addVertex("4", "Person", {"name": "josh", "age": "32"})
    O.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    O.addVertex("6", "Person", {"name": "peter", "age": "35"})
    O.addVertex("7", "Person", {"name": "marko", "age": "35"})

    O.addEdge("1", "3", "created", {"weight": 0.4})
    O.addEdge("1", "2", "knows", {"weight": 0.5})
    O.addEdge("1", "4", "knows", {"weight": 1.0})
    O.addEdge("4", "3", "created", {"weight": 0.4})
    O.addEdge("6", "3", "created", {"weight": 0.2})
    O.addEdge("4", "5", "created", {"weight": 1.0})

    resp = O.listIndices()
    found = False
    for i in resp:
        print("found: %s" % (i))
        if i["field"] == "name":
            found = True
    if not found:
        errors.append("Expected index not found")

    return errors


def test_index_query(O):
    errors = []

    O.addIndex("name")

    O.addVertex("1", "Person", {"name": "marko", "age": "29"})
    O.addVertex("2", "Person", {"name": "mark", "age": "27"})
    O.addVertex("3", "Person", {"name": "mary", "age": "27"})
    O.addVertex("4", "Person", {"name": "josh", "age": "32"})

    count = 0
    for v in O.query().Index(name="mar"):
        if v.gid not in ["1", "2", "3"]:
            errors.append("Index query false hit: %s", v.gid)
        count += 1
    if count != 3:
        errors.append("Incorrect number of hits returned %d != %d" % (count, 3))

    return errors
