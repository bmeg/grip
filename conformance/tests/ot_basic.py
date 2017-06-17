

def test_count(O):
    errors = []

    #O.query().addV("vertex1").property("field1", "value1").property("field2", "value2").execute()
    #O.query().addV("vertex2").execute()
    #O.query().addV("vertex3").property("field1", "value3").property("field2", "value4").execute()
    #O.query().addV("vertex4").execute()

    O.addVertex("vertex1", {"field1" : "value1", "field2" : "value2"})
    O.addVertex("vertex2")
    O.addVertex("vertex3", {"field1" : "value3", "field2" : "value4"})
    O.addVertex("vertex4")

    #O.query().V("vertex1").addE("friend").to("vertex2").execute()
    #O.query().V("vertex2").addE("friend").to("vertex3").execute()
    #O.query().V("vertex2").addE("parent").to("vertex4").execute()
    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")


    count = 0
    for i in O.query().V().execute():
        count += 1
    if count != 4:
        errors.append("Fail: O.query().V() %s != %s" % (count, 4))

    count = 0
    for i in O.query().E().execute():
        count += 1
    if count != 3:
        errors.append("Fail: O.query().E()")

    count = 0
    for i in O.query().V("vertex1").outgoing().execute():
        count += 1
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").outgoing()")

    count = 0
    for i in O.query().V("vertex1").outgoing().outgoing().has("field2", "value4").incoming().execute():
        count += 1
    if count != 1:
        errors.append("""O.query().V("vertex1").outgoing().outgoing().has("field1", "value4")""")

    return errors

def test_outgoing(O):
    errors = []

    #O.query().addV("vertex1").property("field1", "value1").property("field2", "value2").execute()
    #O.query().addV("vertex2").execute()
    #O.query().addV("vertex3").property("field1", "value3").property("field2", "value4").execute()
    #O.query().addV("vertex4").execute()

    O.addVertex("vertex1", {"field1" : "value1", "field2" : "value2"})
    O.addVertex("vertex2")
    O.addVertex("vertex3", {"field1" : "value3", "field2" : "value4"})
    O.addVertex("vertex4")

    #O.query().V("vertex1").addE("friend").to("vertex2").execute()
    #O.query().V("vertex2").addE("friend").to("vertex3").execute()
    #O.query().V("vertex2").addE("parent").to("vertex4").execute()
    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex2", "vertex3", "friend")
    O.addEdge("vertex2", "vertex4", "parent")

    if O.query().V("vertex2").outgoing().count().first()["int_value"] != 2:
        errors.append("blank outgoing doesn't work")

    print O.query().V("vertex2").outgoing("friend").execute()
    print O.query().E().execute()
    if O.query().V("vertex2").outgoing("friend").count().first()["int_value"] != 1:
        errors.append("labeled outgoing doesn't work")

    if O.query().V("vertex2").incoming().count().first()["int_value"] != 1:
        errors.append("blank incoming doesn't work")

    return errors

def test_duplicate(O):
    errors = []
    #O.query().addV("vertex1").execute()
    #O.query().addV("vertex1").execute()
    #O.query().addV("vertex2").execute()
    O.addVertex("vertex1")
    O.addVertex("vertex1")
    O.addVertex("vertex2")

    #O.query().V("vertex1").addE("friend").to("vertex2").execute()
    #O.query().V("vertex1").addE("friend").to("vertex2").execute()
    O.addEdge("vertex1", "vertex2", "friend")
    O.addEdge("vertex1", "vertex2", "friend")

    if O.query().V().count().first()['int_value'] != 2:
        errors.append("duplicate vertex add error")

    if O.query().E().count().first()['int_value'] != 2:
        errors.append("duplicate edge add error")
    return errors
