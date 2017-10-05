

def test_vertex_struct(O):
    errors = []
    #print O.query().addV("vertex1").property("field1", {"test" : 1, "value" : False}).render()
    O.addVertex("vertex1", "person", {"field1" : {"test" : 1, "value" : False}} )
    #print "vertices", O.query().V().execute()
    count = 0
    for i in O.query().V().execute():
        count += 1
        p = i['vertex']['properties']['field1']
        if not isinstance(p,dict):
            errors.append("Dictionary properties failed")
            continue
        if "test" not in p or "value" not in p:
            errors.append("missing keys in structure field")
            continue
        if p["test"] != 1 or p["value"] != False:
            errors.append("Incorrect values in structure")

    if count != 1:
        errors.append("Vertex struct property count failed")

    return errors

def test_edge_struct(O):
    errors = []
    #print O.query().addV("vertex1").property("field1", {"test" : 1, "value" : False}).render()
    #O.query().addV("vertex1").property("field1", {"test" : 1, "value" : False}).execute()
    #O.query().addV("vertex2").property("field1", {"test" : 2, "value" : True}).execute()
    O.addVertex("vertex1", "person", {"field1": {"test" : 1, "value" : False}})
    O.addVertex("vertex2", "person", {"field1": {"test" : 2, "value" : True}} )
    #O.query().V("vertex1").addE("friend").to("vertex2").property("edgevals", {"weight" : 3.14, "count" : 15}).execute()
    O.addEdge("vertex1", "vertex2", "friend", {"edgevals": {"weight" : 3.14, "count" : 15}})

    for i in O.query().V("vertex1").outgoingEdge().execute():
        if 'weight' not in i['edge']['properties']['edgevals'] or i['edge']['properties']['edgevals']['weight'] != 3.14:
            errors.append("out edge properties not found")

    for i in O.query().V("vertex2").incomingEdge().execute():
        if 'weight' not in i['edge']['properties']['edgevals'] or i['edge']['properties']['edgevals']['weight'] != 3.14:
            errors.append("in edge properties not found")

    return errors
