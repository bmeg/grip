

def test_subkey(O):
    """
    Bug in KVGraph scanned edge index prefixes, if key was a prefix subkey of another,
    edge sets would get merged (ie get outgoing from 'Work' and get edges from 'Work' and 'Workflow')
    """
    errors = []

    O.addVertex("Work", "Thing", {})
    O.addVertex("Workflow", "Thing", {})
    O.addVertex("Other", "Thing", {})
    O.addVertex("OtherGuy", "Thing", {})

    O.addEdge("Work", "Other", "edge")
    O.addEdge("Workflow", "OtherGuy", "edge")

    count = 0
    for i in O.query().V("Work").outgoing():
        count += 1
    if count != 1:
        errors.append("Incorrect outgoing vertex count %d != %d" % (count, 1))

    count = 0
    for i in O.query().V("Work").outgoingEdge():
        count += 1
    if count != 1:
        errors.append("Incorrect outgoing edge count %d != %d" % (count, 1))

    count = 0
    for i in O.query().V("Other").incomingEdge():
        count += 1
    if count != 1:
        errors.append("Incorrect incoming edge count %d != %d" % (count, 1))

    return errors
