---
title: Referencing Fields
menu:
  main:
    parent: Queries
    weight: 5
---

Several operations (where, fields, render, etc.) reference properties of the vertices/edges during the traversal.
GRIP uses a variation on JSONPath syntax as described in http://goessner.net/articles/ to reference fields during traversals.

The following query:

```
O.query().V(["ENSG00000012048"]).as_("gene").out("variant")
```

Starts at vertex `ENSG00000012048` and marks as `gene`:

```
{
  "gid": "ENSG00000012048",
  "label": "gene",
  "data": {
    "symbol": {
      "ensembl": "ENSG00000012048",
      "hgnc": 1100,
      "entrez": 672,
      "hugo": "BRCA1"
    }
    "transcipts": ["ENST00000471181.7", "ENST00000357654.8", "ENST00000493795.5"]
  }
}
```

as "gene" and traverses the graph to:

```
{
  "gid": "NM_007294.3:c.4963_4981delTGGCCTGACCCCAGAAG",
  "label": "variant",
  "data": {
    "type": "deletion"
    "publications": [
      {
        "pmid": 29480828,
        "doi": "10.1097/MD.0000000000009380"
      },
      {
        "pmid": 23666017,
        "doi": "10.1097/IGC.0b013e31829527bd"
      }
    ]
  }
}
```

Below is a table of field and the values they would reference in subsequent traversal operations.

| jsonpath                   | result               |
| :------------------------- | :------------------- |
| _gid                       | "NM_007294.3:c.4963_4981delTGGCCTGACCCCAGAAG" |
| _label                     | "variant"            |
| _data.type                 | "deletion"           |
| type                       | "deletion"           |
| publications[0].pmid       | 29480828             |
| publications[:].pmid       | [29480828, 23666017] |
| publications.pmid          | [29480828, 23666017] |
| $gene.symbol.hugo          | "BRCA1"              |
| $gene.transcripts[0]       | "ENST00000471181.7"  |


## Usage Example:

```
O.query().V(["ENSG00000012048"]).as_("gene").out("variant").render({"variant_id": "_gid", "variant_type": "type", "gene_id": "$gene._gid"})
```

returns

```
{
  "variant_id": "NM_007294.3:c.4963_4981delTGGCCTGACCCCAGAAG",
  "variant_type": "deletion",
  "gene_id": "ENSG00000012048"
}
```
