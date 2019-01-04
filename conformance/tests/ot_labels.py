

def test_list_labels(O):
    errors = []

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

    resp = O.listLabels()
    print(resp)
    if len(resp["vertex_labels"]) != 2:
        errors.append("listLabels returned an unexpected number of vertex labels; %d != 2" % (len(resp["vertex_labels"])))
    if sorted(resp["vertex_labels"]) != ["Person", "Software"]:
        errors.append("listLabels returned unexpected vertex labels")
    if len(resp["edge_labels"]) != 2:
        errors.append("listLabels returned an unexpected number of edge labels; %d != 2" % (len(resp["edge_labels"])))
    if sorted(resp["edge_labels"]) != ["created", "knows"]:
        errors.append("listLabels returned unexpected edge labels")

    return errors
