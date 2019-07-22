
<!-- README.md is generated from README.Rmd. Please edit that file -->
ophion
======

Overview
--------

Installation
------------

To install from github:

``` r
library(devtools)
install_github(repo="bmeg/ophion", subdir="client/R")
```

To install locally:

``` bash
git clone https://github.com/bmeg/ophion.git
cd ophion/client/R
R CMD INSTALL .
```

Usage
-----

``` r
library(ophion)
library(magrittr)

# Preview a query
ophion("bmeg.io") %>%
    query() %>%
    has("gid", "cohort:TCGA-READ") %>%
    outgoing("hasSample") %>%
    incoming("expressionFor") %>%
    render()

# Execute a query
ophion("bmeg.io") %>% 
  query() %>%
  has("gid", within(c("CCLE:OCIM1_HAEMATOPOIETIC_AND_LYMPHOID_TISSUE", "biosample:CCLE:JHUEM2_ENDOMETRIUM"))) %>% 
  execute()
```
