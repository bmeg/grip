---
title: TCGA RNA Expression

menu:
  main:
    parent: Tutorials
    weight: 2
---

### Explore TCGA RNA Expression Data

Create the graph

```
arachne create tcga-rna
```

Load pathway information

```
curl -O http://www.pathwaycommons.org/archives/PC2/v9/PathwayCommons9.All.hgnc.sif.gz
gunzip PathwayCommons9.All.hgnc.sif.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_sif.py --db tcga-rna PathwayCommons9.All.hgnc.sif
```

Load expression data

```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/HiSeqV2.gz
gunzip HiSeqV2.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_matrix.py --db tcga-rna HiSeqV2
```

Load clinical information

```
curl -O https://tcga.xenahubs.net/download/TCGA.BRCA.sampleMap/BRCA_clinicalMatrix.gz
gunzip BRCA_clinicalMatrix.gz
python $GOPATH/src/github.com/bmeg/arachne/example/load_property_matrix.py --db tcga-rna BRCA_clinicalMatrix
```

Query the graph

```
pip install "git+https://github.com/bmeg/arachne.git#egg=aql&subdirectory=aql/python/"
```

```python
import aql

conn = aql.Connection("http://localhost:8201")
O = conn.graph("tcga-rna")

# Print out expression data of all Stage IIA samples
for row in O.query().\
    V().\
    where(aql.and_(aql.eq("_label", "Sample"), aql.eq("pathologic_stage", "Stage IIA"))).\
    out("has").\
    where(aql.eq("_label", "Data:Expression"):
  print row
```
