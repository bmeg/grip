

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

    count = 0
    for i in O.query().V().distinct():
        count += 1
    if count != 13:
        errors.append("Distinct %s != %s" % (count, 13))

    count = 0
    for i in O.query().V().distinct("$.gid"):
        count += 1
    if count != 13:
        errors.append("Distinct %s != %s" % (count, 13))

    count = 0
    for i in O.query().V().distinct("$.name"):
        count += 1
    if count != 11:
        errors.append("Distinct %s != %s" % (count, 11))

    return errors
