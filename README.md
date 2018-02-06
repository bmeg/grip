The Arachne Graph Database server

To Install
----------
```
go get github.com/bmeg/arachne
```
If you have defined `$GOPATH` the application will be installed at
`$GOPATH`/bin/arachne otherwise it will be `$HOME/go/bin/arachne`



To Turn on server
-----------------
```
arachne server
```


To Run Larger 'Amazon Data Test'
--------------------------------

Turn on local arachne server


Download test data
```
curl -O http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz
```

Convert the data
```
python $GOPATH/src/github.com/bmeg/arachne/test/amazon_convert.py amazon-meta.txt.gz test.data
```

Create Amazon Graph
```
arachne create amazon
```

List the Graphs
```
arachne list
```

Load data
```
arachne load --edge test.data.edge --vertex test.data.vertex --graph amazon
```


Some example queries
```
import aql
import json

conn = aql.Connection("http://localhost:8201")

O = conn.graph("amazon")

#Count the Vertices
print list(O.query().V().count().execute())
#Count the Edges
print list(O.query().E().count().execute())

#Try simple traveral
#print O.query().V("B00000I06U").outEdge().execute()


#Do a group count of the different 'group's in the graph
print O.query().V().groupCount("group").execute()

#use graph to find every Book that is similar to a DVD
for a in O.query().V().has("group", "Book").mark("a").outgoing("similar").has("group", "DVD").mark("b").select(["a", "b"]).execute():
    print a
```

Matrix Data loading Example
---------------------------

Create the graph
```
arachne create test-data
```

Add aql.py Python Library to PYTHONPATH
```
export PYTHONPATH=`pwd`
```

Install Pandas if you don't already have it
```
pip install pandas
```

Load Pathway information
```
curl -O http://www.pathwaycommons.org/archives/PC2/v9/PathwayCommons9.All.hgnc.sif.gz
gunzip PathwayCommons9.All.hgnc.sif.gz
python ./test/load_sif.py PathwayCommons9.All.hgnc.sif
```

Load Matrix data
```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/HiSeqV2.gz
gunzip HiSeqV2.gz
python ./test/load_matrix.py HiSeqV2
```

Load clinical information
```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/BRCA_clinicalMatrix.gz
gunzip BRCA_clinicalMatrix.gz
python ./test/load_property_matrix.py BRCA_clinicalMatrix
```



Python Query: Open Connection
```
import aql
conn = aql.Connection("http://localhost:8201")
O = conn.graph("test-data")
```

Print out expression data of all Stage IIA samples
```
for row in O.query().V().hasLabel("Sample").has("pathologic_stage", "Stage IIA").outgoing("has").hasLabel("Data:expression").outgoingBundle("value"):
  print row
```
