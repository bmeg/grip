
<!-- README.md is generated from README.Rmd. Please edit that file -->

# gripql

## Overview

## Installation

To install from github:

``` r
library(devtools)
install_github(repo="bmeg/grip", subdir="gripql/R")
```

To install locally:

``` bash
git clone https://github.com/bmeg/grip.git
cd grip/gripql/R
R CMD INSTALL .
```

## Usage

``` r
library(gripql)
library(magrittr)

# Preview a query
gripql("bmeg.io") %>%
    query() %>%
    V() %>%
    has(eq("_gid", "Project::TCGA-READ")) %>%
    out("cases") %>%
    out("samples") %>%
    out("aliquots") %>%
    to_json()

# Execute a query
gripql("bmeg.io") %>%
  query() %>%
  V() %>%
  has(within("_gid", c("CCLE:OCIM1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE", "biosample:CCLE:JHUEM2_ENDOMETRIUM"))) %>% 
  execute()
```
