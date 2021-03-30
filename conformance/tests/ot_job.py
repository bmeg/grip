from __future__ import absolute_import

import gripql
import time

def test_job(man):
    errors = []

    G = man.setGraph("swapi")
    job = G.query().V().hasLabel("Planet").as_("a").out().submit()

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

    jobs = G.query().V().hasLabel("Planet").as_("a").out().out().count().searchJobs()
    count = 0
    for cJob in jobs:
        if cJob["id"] != job["id"]:
            errors.append("Wrong job found")
        else:
            count += 1
    if count != 1:
        errors.append("Job not found in search: %d" % (count))

    fullResults = []
    for res in G.query().V().hasLabel("Planet").out().out().count():
        fullResults.append(res)

    resumedResults = []
    for res in G.resume(job["id"]).out().count().execute():
        resumedResults.append(res)

    if len(fullResults) != len(resumedResults):
        errors.append( "Missmatch on resumed result" )

    fullResults = []
    for res in G.query().V().hasLabel("Planet").as_("a").out().out().select("a"):
        fullResults.append(res)
    #TODO: in the future, this 'fix' may need to be removed.
    #Always producing elements in the same order may become a requirement.
    fullResults.sort(key=lambda x:x["gid"])
    resumedResults = []
    for res in G.resume(job["id"]).out().select("a").execute():
        resumedResults.append(res)
    resumedResults.sort(key=lambda x:x["gid"])

    if len(fullResults) != len(resumedResults):
        errors.append( "Missmatch on resumed result" )

    for a, b in zip(fullResults, resumedResults):
        if a != b:
            errors.append("%s != %s" % (a, b))

    G.deleteJob(job["id"])
    count = 0
    for j in G.listJobs():
        if job['id'] == j['id']:
            count += 1
    if count != 0:
        errors.append("Job not deleted")

    return errors
