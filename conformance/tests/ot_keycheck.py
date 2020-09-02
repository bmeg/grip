

def test_subkey(man):
    """
    Bug in KVGraph scanned edge index prefixes, if key was a prefix subkey of another,
    edge sets would get merged (ie get outgoing from 'Work' and get edges from 'Work' and 'Workflow')
    """
    errors = []

    G = man.writeTest()

    G.addVertex("Work", "Thing", {})
    G.addVertex("Workflow", "Thing", {})
    G.addVertex("Other", "Thing", {})
    G.addVertex("OtherGuy", "Thing", {})

    G.addEdge("Work", "Other", "edge")
    G.addEdge("Workflow", "OtherGuy", "edge")

    count = 0
    for i in G.query().V("Work").out():
        count += 1
    if count != 1:
        errors.append("Incorrect outgoing vertex count %d != %d" % (count, 1))

    count = 0
    for i in G.query().V("Work").outE():
        count += 1
    if count != 1:
        errors.append("Incorrect outgoing edge count %d != %d" % (count, 1))

    count = 0
    for i in G.query().V("Other").inE():
        count += 1
    if count != 1:
        errors.append("Incorrect incoming edge count %d != %d" % (count, 1))

    return errors
