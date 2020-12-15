
# Genomic Data Commons Example


## Setup

Launch GDC Proxy Server
```
./gdc_grip_proxy.py
```

Launch GRIP
```
grip server --config grip-gdc.yaml
```


## Test Queries

```
V().hasLabel("GDCCase")
```

```
V("gdc.cancer.gov/case/eb20d954-01a0-4fe0-a201-e37b917f0fc0")
```

```
V("gdc.cancer.gov/case/c43c8eea-7014-4abe-b805-397a9062d3e0").out("ssms")
```

```
V("gdc.cancer.gov/ssm_occurrence/05c1bca0-c401-5d55-af7c-f7b38959488c")
```

```
V().hasLabel("PDCPublicCase").out("gdcRecord").limit(5).out("ssms")
```

```
V().hasLabel("PDCPublicCase").out("gdcRecord").limit(5).out("ssms").in_("occurances")'
```

```
V().hasLabel("GDCProject").has(contains("disease_type", "Myeloid Leukemias"))
```

```
V("gdc.cancer.gov/case/cd7fb75e-1c2e-4201-86d3-b3ba21f476bf").in_("cases")
```

```
V("gdc.cancer.gov/project/TCGA-BRCA").out("cases").in_("gdcRecord")
```

```
V("pdc.esacinc.com/public_case/0939ac6b-63d8-11e8-bcf1-0a2705229b82").out("gdcRecord").out("ssms").count()
```
