from __future__ import absolute_import

import gripql


def test_pivot(man):
    errors = []
    G = man.setGraph("fhir")

    for row in G.query().V().hasLabel("Patient").as_("a").out("patient_observation").pivot("$a._gid", "$.key", "$.value" ):
        print(row)

    return errors

