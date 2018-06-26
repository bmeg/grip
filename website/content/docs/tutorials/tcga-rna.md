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

Get the data
```
curl -O http://download.cbioportal.org/gbm_tcga_pub2013.tar.gz
tar xvzf gbm_tcga_pub2013.tar.gz
```

Load clinical data
```
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_clinical.txt --row-label 'Donor'
```

Load RNASeq data
```
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_RNA_Seq_v2_expression_median.txt -t  --index-col 1 --row-label RNASeq --row-prefix "RNA:" --exclude RNA:Hugo_Symbol
```

Connect RNASeq data to Clinical data
```
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_RNA_Seq_v2_expression_median.txt -t  --index-col 1 --no-vertex --edge 'RNA:{_gid}' rna
```

Connect Clinical data to subtypes
```
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_clinical.txt --no-vertex -e "{EXPRESSION_SUBTYPE}" subtype --dst-vertex "{EXPRESSION_SUBTYPE}" Subtype -d
```

Load EntrezID to Hugo Symbol mapping
```
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_RNA_Seq_v2_expression_median.txt --index-col 1 --column-include Hugo_Symbol --row-label Gene
```


Load Proneural samples into a matrix
```python
import pandas
import aql

conn = aql.Connection("http://localhost:8201")
O = conn.graph("tcga-rna")
genes = {}
for k, v in O.query().V().where(aql.eq("_label", "Gene")).render(["_gid", "Hugo_Symbol"]):
    genes[k] = v
data = {}
for row in O.query().V("Proneural").in_().out("rna").render(["_gid", "_data"]):
    data[row[0]] = row[1]
samples = pandas.DataFrame(data).rename(genes).transpose().fillna(0.0)
```
