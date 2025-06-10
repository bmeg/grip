import gripql


def test_index(man):
    errors = []

    G = man.writeTest()
    G.addIndex("Person", "name")

    G.addVertex("1", "Person", {"name": "marko", "age": "29"})
    G.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    G.addVertex("3",  "Software", {"name": "lop", "lang": "java"})
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

    count = 0
    for i in G.query().V().has(gripql.eq("name","marko")):
        count += 1
        if "name" not in i:
            errors.append("'name' field not found in vertex")
        if i["name"] != "marko":
            errors.append("Filtering on field name, value marko but got '%s' instead" % i["name"])
    if count != 2:
        errors.append("Expecting 2 vertices returned but got %d instead" % (count))
    return errors


def test_bulk_index(man):
    errors = []

    G = man.writeTest()
    G.addIndex("Person", "age")

    bulk = G.bulkAdd()

    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})
    bulk.addVertex("7", "Person", {"name": "alice", "age": "31"})
    bulk.addVertex("8", "Person", {"name": "bob", "age": "32"})
    bulk.addVertex("9", "Person", {"name": "charlie", "age": "28"})
    bulk.addVertex("10", "Person", {"name": "diana", "age": "32"})
    bulk.addVertex("11", "Person", {"name": "eve", "age": "30"})
    bulk.addVertex("12", "Person", {"name": "frank", "age": "33"})
    bulk.addVertex("13", "Person", {"name": "grace", "age": "26"})
    bulk.addVertex("14", "Person", {"name": "heidi", "age": "32"})
    bulk.addVertex("15", "Person", {"name": "ivan", "age": "29"})
    bulk.addVertex("16", "Person", {"name": "judy", "age": "34"})


    res = bulk.execute()

    count = 0
    resp3 = G.query().V().has(gripql.eq("age","32"))
    for i in resp3:
        count += 1
        if "age" not in i:
            errors.append("field 'age' not found in vertex")
        if "age" in i and i["age"] != "32":
            errors.append("filtering on field age value '32' but got %s instead" %s (i["age"]))
    if count != 4:
        errors.append("expected count 4 but got %d instead" % (count))

    return errors


def test_index_after_write(man):
    errors = []

    G = man.writeTest()

    bulk = G.bulkAdd()
    bulk.addVertex("1", "Person", {"name": "marko", "age": "29"})
    bulk.addVertex("2", "Person", {"name": "vadas", "age": "27"})
    bulk.addVertex("4", "Person", {"name": "josh", "age": "32"})
    bulk.addVertex("6", "Person", {"name": "peter", "age": "35"})
    bulk.addVertex("7", "Person", {"name": "alice", "age": "31"})
    bulk.addVertex("8", "Person", {"name": "bob", "age": "32"})
    bulk.addVertex("9", "Person", {"name": "charlie", "age": "28"})
    bulk.addVertex("10", "Person", {"name": "diana", "age": "32"})
    bulk.addVertex("11", "Person", {"name": "eve", "age": "30"})
    bulk.addVertex("12", "Person", {"name": "frank", "age": "33"})
    bulk.addVertex("13", "Person", {"name": "grace", "age": "26"})
    bulk.addVertex("14", "Person", {"name": "heidi", "age": "32"})
    bulk.addVertex("15", "Person", {"name": "ivan", "age": "29"})
    bulk.addVertex("16", "Person", {"name": "judy", "age": "34"})
    res = bulk.execute()
    if res["errorCount"] > 0:
        errors.append("errorCount on bulk add > 0")

    G.addIndex("Person", "age")

    count = 0
    restwo = G.query().V().has(gripql.within("name", ["marko", "vadas", "eve", "ivan", "charlie", "nothere"]))
    for i in restwo:
        count += 1
    if count != 5:
        errors.append("Expected 5 names from filter but got %d instead" % (count))

    return errors


def test_index_filter(man):
    errors = []
    G = man.setGraph("swapi")

    G.addIndex("Starship", "cost_in_credits")
    G.addIndex("Starship", "cargo_capacity")


    respthree = G.query().V().hasLabel("Starship").has(gripql.lt("cost_in_credits", 150000000))
    count = 0
    for i in respthree:
        count += 1
        if i['cost_in_credits'] > 149999999:
            errors.append("filtering on ships that cost less than 150000000, but %d > 149999999" % (i['cost_in_credits']))

    if count != 5:
        errors.append("Expected 5 results got %d instead" % (count))

    indices = G.listIndices()
    found = False
    count = 0
    for i in indices:
        count +=1
        if i["field"] == "cost_in_credits" and i["label"] == "Starship":
            found = True
    if not found:
        errors.append("Expected index not found")
    if count != 2:
        errors.append("Expected to find 2 indices but found %d instead" % (count))

    G.deleteIndex("Starship", "cost_in_credits")
    count = 0
    indices_two = G.listIndices()
    found = False
    for i in indices_two:
        count += 1
        if i["field"] == "cost_in_credits" and i["label"] == "Starship":
            found = True
    if found:
        errors.append("Expected index not found, but it was found")
    if count != 1:
        errors.append("Expected to find 1 index but found %d instead" % (count))

    return errors


def test_consistent_results(man):
    errors = []
    G = man.setGraph("swapi")

    resp = G.query().V().has(gripql.contains("eye_colors", "yellow"))
    count = 0
    for i in resp:
        count += 1
    if count != 2:
        errors.append("Expected 2 results but got %d instead" % (count))

    G.addIndex("Species", "eye_colors")
    resp = G.query().V().has(gripql.contains("eye_colors", "yellow"))
    count = 0
    for i in resp:
        count += 1
    if count != 2:
        errors.append("Expected 2 results but got %d instead" % (count))

    return errors
