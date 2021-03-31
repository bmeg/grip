---
title: Jobs API
menu:
  main:
    parent: Queries
    weight: 20
---

# Jobs API

Not all queries return instantaneously, additionally some queries elements are used
repeatedly. The query Jobs API provides a mechanism to submit graph traversals
that will be evaluated asynchronously and can be retrieved at a later time.


### Submitting a job

```
job = G.query().V().hasLabel("Planet").out().submit()
```

### Getting job status
```
jinfo = G.getJob(job["id"])
```


### Reading job results
```
for row in G.readJob(job["id"]):
   print(row)
```

### Search for jobs

Find jobs that match the prefix of the current request (example should find job from G.query().V().hasLabel("Planet").out())

```
jobs = G.query().V().hasLabel("Planet").out().out().count().searchJobs()
```

If there are multiple jobs that match the prefix of the search, all of them will be returned. It will be a client side
job to decide which of the jobs to use as a starting point. This can either be the job with the longest matching prefix, or
the most recent job. Note, that if the underlying database has changed since the job was run, adding additional steps to the
traversal may produce inaccurate results.

Once `job` has been selected from the returned list you can use these existing results and continue the traversal.

```
for res in G.resume(job["id"]).out().count():
    print(res)
```
