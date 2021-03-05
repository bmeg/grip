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
    
    if count != 5:
        errors.append("Incorrect # elements returned")
        
    G.deleteJob(job["id"])
    count = 0
    for j in G.listJobs():
        if job['id'] == j['id']:
            count += 1
    if count != 0:
        errors.append("Job not deleted")

    return errors
