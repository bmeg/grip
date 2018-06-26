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
./example/load_matrix.py tcga-rna gbm_tcga_pub2013/data_clinical.txt --no-vertex -e "{EXPRESSION_SUBTYPE}" subtype --dst-vertex "{EXPRESSION_SUBTYPE}" Subtype
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


# Matrix Load project

```
usage: load_matrix.py [-h] [--sep SEP] [--server SERVER]
                      [--row-label ROW_LABEL] [--row-prefix ROW_PREFIX] [-t]
                      [--index-col INDEX_COL] [--connect]
                      [--col-label COL_LABEL] [--col-prefix COL_PREFIX]
                      [--edge-label EDGE_LABEL] [--edge-prop EDGE_PROP]
                      [--columns [COLUMNS [COLUMNS ...]]]
                      [--column-include COLUMN_INCLUDE] [--no-vertex]
                      [-e EDGE EDGE] [--dst-vertex DST_VERTEX DST_VERTEX]
                      [-x EXCLUDE] [-d]
                      db input

positional arguments:
  db                    Destination Graph
  input                 Input File

optional arguments:
  -h, --help            show this help message and exit
  --sep SEP             TSV delimiter
  --server SERVER       Server Address
  --row-label ROW_LABEL
                        Vertex Label used when loading rows
  --row-prefix ROW_PREFIX
                        Prefix added to row vertex gid
  -t, --transpose       Transpose matrix
  --index-col INDEX_COL
                        Column number to use as index (and gid for vertex
                        load)
  --connect             Switch to 'fully connected mode' and load matrix cell
                        values on edges between row and column names
  --col-label COL_LABEL
                        Column vertex label in 'connect' mode
  --col-prefix COL_PREFIX
                        Prefix added to col vertex gid in 'connect' mode
  --edge-label EDGE_LABEL
                        Edge label for edges in 'connect' mode
  --edge-prop EDGE_PROP
                        Property name for storing value when in 'connect' mode
  --columns [COLUMNS [COLUMNS ...]]
                        Rename columns in TSV
  --column-include COLUMN_INCLUDE
                        List subset of columns to use from TSV
  --no-vertex           Do not load row as vertex
  -e EDGE EDGE, --edge EDGE EDGE
                        Create an edge the connected the current row vertex
                        args: <dst> <edgeType>
  --dst-vertex DST_VERTEX DST_VERTEX
                        Create a destination vertex, args: <dstVertex>
                        <vertexLabel>
  -x EXCLUDE, --exclude EXCLUDE
                        Exclude row id
  -d                    Run in debug mode. Print actions and make no changes

```
