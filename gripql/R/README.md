
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
    graph("bmeg_rc2") %>%
    query() %>%
    V() %>%
    hasLabel("Project") %>%
    out("cases") %>% as_("c") %>%
    out("samples") %>% as_("s") %>%
    out("aliquots") %>% as_("a") %>%
    render(list("case_id" = "$c._gid", "sample_id" = "$s._gid", "aliquot_id" = "$a._gid"))
    to_json()

# Execute a query
gripql("bmeg.io") %>%
    graph("bmeg_rc2") %>%
    query() %>%
    V() %>%
    hasLabel("Aliquot") %>%
    has(eq("sample_type", "Primary Tumor")) %>% 
    limit(10) %>%
    execute()
```
