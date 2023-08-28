# Graphql Grip Endpoint Gen3 Installation instructions
In addition to cloning this repo you will also need to have a running [gen3 helm deployment](https://github.com/ACED-IDP/gen3-helm/tree/feature/grip).

Once you have followed the gen3helm deployment instructions, and have running grip and mongodb pods
you will need to exec into the grip pod to load the data into mongo and start the server:

Get a list of all running pods to make sure grip pod is running
```
kubectl get pods
```

copy the config, data, and files into the grip pod with:
```
kubectl cp graphql_gen3.so  local-grip-your_unique_hash:/data
kubectl cp mongo.yml  local-grip-your_unique_hash:/data
```
The shared object file should have been built with the image and should already be in /data

Exec into grip pod with: 
```
kubectl exec --stdin --tty deployment/local-grip -- /bin/bash
cd data
grip server -w api/graphql=graphql_gen3.so -c mongo.yml
```

Create a new tab and exec into the same pod with the same command above, then run the below commands to 
import data into mongo, generate a schema from the populated data in mongo and post it to the graphql endpoint:

```
grip create synthea
grip server load --vertex output/Observation_new.ndjson
grip server load --vertex output/Patient_new.ndjson
grip server load --vertex output/DocumentReference_new.ndjson
grip schema sample synthea2 > synthea2.schema.json
grip schema post --json synthea2.schema.json
```
Note: output/ is the directory that contains the bare minimum 3 vertex data files that are needed to display data on the exploration page.