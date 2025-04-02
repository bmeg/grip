from __future__ import absolute_import

import gripql


def test_pivot(man):
    errors = []
    G = man.setGraph("fhir")

    ## TODO: better result checking
    count = 0
    for row in G.query().V().hasLabel("Patient").as_("a").out("patient_observation").pivot("$a._id", "$.key", "$.value" ):
        if row["_id"] not in ["patient_a", "patient_b"]:
            errors.append("Unexpected id: %s" % (row["_id"]))
        count += 1

    if count == 0:
        errors.append("nothing to return")



    return errors
