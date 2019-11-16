from __future__ import absolute_import

import requests

"""
Queries that are designed to test the path traversal optimization
some engines (ie GRIDS) will do. Can't verify that the optimization
was applied, but does verify that the results seem correct
"""

def setupGraph(O):
    O.addVertex("vertex1_1", "step1", {"field1": "value1", "field2": "value2"})
    O.addVertex("vertex1_2", "step1")
    O.addVertex("vertex1_3", "step1", {"field1": "value3", "field2": "value4"})

    O.addVertex("vertex2_1", "step2", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex2_2", "step2", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex2_3", "step2", {"field1": "value3", "field2": "value4"})

    O.addVertex("vertex3_1", "step3", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex3_2", "step3", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex3_3", "step3", {"field1": "value3", "field2": "value4"})

    O.addVertex("vertex4_1", "step4", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4_2", "step4", {"field1": "value3", "field2": "value4"})
    O.addVertex("vertex4_3", "step4", {"field1": "value3", "field2": "value4"})

    O.addEdge("vertex1_1", "vertex2_1", "step", gid="edge1")
    O.addEdge("vertex1_2", "vertex2_2", "step", gid="edge2")
    O.addEdge("vertex1_3", "vertex2_3", "step")

    O.addEdge("vertex2_1", "vertex3_1", "step", gid="edge3")
    O.addEdge("vertex2_2", "vertex3_2", "step", gid="edge4")
    O.addEdge("vertex2_3", "vertex3_3", "step")

    O.addEdge("vertex3_1", "vertex4_1", "step", gid="edge5")
    O.addEdge("vertex3_2", "vertex4_2", "step", gid="edge6")
    O.addEdge("vertex3_3", "vertex4_3", "step")


def test_path(O):
    errors = []
    setupGraph(O)
    """
    count = 0
    for res in O.query().V().out().out().out():
        if not res.gid.startswith("vertex4"):
            errors.append("Wrong vertex found at end of path: %s" % (res.gid))
        if not res.label == "step4":
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 3:
        errors.append("Incorrect vertex count returned: %d != %d" % (count, 3) )

    count = 0
    for res in O.query().V().in_().in_().in_():
        if not res.gid.startswith("vertex1"):
            errors.append("Wrong vertex found at end of path: %s" % (res.gid))
        if not res.label == "step1":
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 3:
        errors.append("Incorrect vertex count returned: %d != %d" % (count, 3) )

    count = 0
    for res in O.query().V().out().out().outE():
        if not res['from'].startswith("vertex3"):
            errors.append("Wrong 'from' vertex found at end of outE path: %s" % (res['from']))
        if not res['to'].startswith("vertex4"):
            errors.append("Wrong 'to' vertex found at end of ourE path: %s" % (res['to']))
        if not res.label == "step":
            errors.append("Wrong label found at end of path: %s" % (res.label))
        count += 1
    if count != 3:
        errors.append("Incorrect vertex count returned: %d != %d" % (count, 3) )

    for res in O.query().V().out().out().outE():
        print(res)
    """
    count = 0
    for res in O.query().V().out().out().outE().out():
        print(res)
        if not res.gid.startswith("vertex4"):
            errors.append("Wrong vertex found at end of outE to out path: %s" % (res.gid))
        if not res.label == "step4":
            errors.append("Wrong label found at end of outE to out path: %s" % (res.label))
        count += 1
    if count != 3:
        errors.append("Incorrect vertex count returned: %d != %d" % (count, 3) )
    """
    for res in O.query().V().out().hasLabel("step2").out().out():
        if not res.gid.startswith("vertex4"):
            errors.append("Wrong vertex found at end of hasLabel path: %s" % (res.gid))
        count += 1
    if count != 3:
        errors.append("Incorrect vertex count returned: %d != %d" % (count, 3) )

    """
    return errors
