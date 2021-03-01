from __future__ import absolute_import

import gripql


def test_job(man):
    errors = []

    G = man.setGraph("swapi")
    job = G.query().V().hasLabel("Planet").out().submit()

    print(job)

    count = 0
    for j in G.listJobs():
        if job == j:
            count += 1
    if count != 1:
        errors.append("Wrong job counts: %s != %s", count, 1)


    if count != 5:
        errors.append("Incorrect # elements returned")

    return errors