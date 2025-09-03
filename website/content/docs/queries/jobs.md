


## .submit()
Post the traversal as an asynchronous job and get a job ID.

Example: Submit a query to be processed in the background

```python
job_id = G.V('vertexID').hasLabel('Vertex').submit()
print(job_id)  # print job ID
```
---

## .searchJobs()
Find jobs that match this query and get their status and results if available.

Example: Search for jobs with the specified query and print their statuses and results

```python
for result in G.V('vertexID').hasLabel('Vertex').searchJobs():
    print(result['status'])  # print job status
    if 'results' in result:
        print(result['results'])  # print job results
```
---
