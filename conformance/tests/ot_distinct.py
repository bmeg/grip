import gripql


def test_distinct(O):
    errors = []

    O.addVertex("1", "Person", {"name": "marko", "age": 29})
    O.addVertex("2", "Person", {"name": "vadas", "age": 25})
    O.addVertex("4", "Person", {"name": "josh", "age": 32})
    O.addVertex("6", "Person", {"name": "peter", "age": 35})
    O.addVertex("7", "Person", {"name": "marko", "age": 41})
    O.addVertex("9", "Person", {"name": "alex", "age": 30})
    O.addVertex("10", "Person", {"name": "alex", "age": 45})
    O.addVertex("11", "Person", {"name": "steve", "age": 26})
    O.addVertex("12", "Person", {"name": "alice", "age": 22})
    O.addVertex("13", "Person", {"name": "wanda", "age": 36})
    O.addVertex("5", "Software", {"name": "ripple", "lang": "java"})
    O.addVertex("3", "Software", {"name": "lop", "lang": "java"})
    O.addVertex("8", "Software", {"name": "funnel", "lang": "go"})
    O.addVertex("14", "Software", {"name": "grip", "lang": None})

    O.addEdge("1", "5", "developer", gid="edge1")
    O.addEdge("7", "5", "developer", gid="edge2")
    O.addEdge("3", "8", "dependency", gid="edge3")

    count = 0
    for i in O.query().V().distinct():
        count += 1
    if count != 14:
        errors.append("Distinct %s != %s" % (count, 14))

    count = 0
    for i in O.query().V().distinct("_gid"):
        count += 1
    if count != 14:
        errors.append("Distinct %s != %s" % (count, 14))

    count = 0
    for i in O.query().V().distinct("name"):
        count += 1
    if count != 12:
        errors.append("Distinct %s != %s" % (count, 12))

    count = 0
    for i in O.query().V().distinct("lang"):
        count += 1
    if count != 3:
        errors.append("Distinct %s != %s" % (count, 3))

    count = 0
    for i in O.query().V().distinct("non-existent-field"):
        count += 1
    if count != 0:
        errors.append("Distinct %s != %s" % (count, 0))

    count = 0
    for i in O.query().V().where(gripql.eq("_label", "Person")).mark("person").out().distinct("$person.name"):
        count += 1
    if count != 1:
        errors.append("Distinct %s != %s" % (count, 1))

    return errors
