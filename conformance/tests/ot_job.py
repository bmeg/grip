from __future__ import absolute_import

import gripql
import time

def test_job(man):
    errors = []

    G = man.setGraph("swapi")
    job = G.query().V().hasLabel("Planet").out().submit()

    count = 0
    for j in G.listJobs():
        if job['id'] == j['id']:
            count += 1
    if count != 1:
        errors.append("Job not found: %s != %s" % (count, 1))

    while True:
        cJob = G.getJob(job["id"])
        if cJob['state'] not in ["RUNNING", "QUEUED"]:
            break
        time.sleep(1)

    count = 0
    for row in G.readJob(job["id"]):
        count += 1

    if count != 12:
        errors.append("Incorrect # elements returned %d != %d" % (count, 12))

    jobs = G.query().V().hasLabel("Planet").out().out().count().searchJobs()
    count = 0
    for cJob in jobs:
        if cJob["id"] != job["id"]:
            errors.append("Wrong job found")
        else:
            count += 1
    if count != 1:
        errors.append("Job not found in search")

    fullResults = []
    for res in G.query().V().hasLabel("Planet").out().out().count():
        fullResults.append(res)

    resumedResults = []
    for res in G.resume(job["id"]).out().count().execute(debug=True):
        resumedResults.append(res)

    if len(fullResults) != len(resumedResults):
        errors.append( "Missmatch on resumed result" )

    G.deleteJob(job["id"])
    count = 0
    for j in G.listJobs():
        if job['id'] == j['id']:
            count += 1
    if count != 0:
        errors.append("Job not deleted")

    return errors
