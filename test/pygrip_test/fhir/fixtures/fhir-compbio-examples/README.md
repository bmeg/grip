##### META folder test-data:

```
>>>> resources={'summary': {'DocumentReference': 1, 'Specimen': 1, 'Observation': 3, 'ResearchStudy': 1, 'ResearchSubject': 1, 'Organization': 1, 'Patient': 1}}
```

There are three Observations with user-defined metadata component. 
1. Focus - reference -> Specimen
2. Focus - reference -> DocumentReference 
   1. The first Observation contains metadata on the file's sequencing metadata.
   2. The second Observation includes a simple summary of a CNV analysis result computed from this file.
