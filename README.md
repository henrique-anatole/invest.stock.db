
<!-- README.md is generated from README.Rmd. Please edit that file -->

# invest.stock.db

<!-- badges: start -->

<!-- badges: end -->

The goal of invest.stock.db is to support the creation of a structured
database for stock data to facilitate investment analysis.

## Installation

You can install the development version of this package from GitHub:

``` r
# install.packages("remotes")
remotes::install_github("henrique-anatole/invest.stock.db", dependencies = TRUE)
```

## Step by step example

The step by step below will allow you to create your own database. The
invest.stock.db package will create it using duckdb and save it in a
single file you can later use to connect and query. Therefore, the file
name and path are the first variables to define, and this will be used
to create the database connection. You can clone a project implementing
it here: <https://github.com/henrique-anatole/stock_data_db>

``` r
# load the package
library(invest.stock.db)
#> Warning: replacing previous import 'readr::guess_encoding' by
#> 'rvest::guess_encoding' when loading 'invest.data'
#> Registered S3 method overwritten by 'quantmod':
#>   method            from
#>   as.zoo.data.frame zoo
library(tidyverse)
#> ── Attaching core tidyverse packages ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── tidyverse 2.0.0 ──
#> ✔ dplyr     1.2.1     ✔ readr     2.2.0
#> ✔ forcats   1.0.1     ✔ stringr   1.6.0
#> ✔ ggplot2   4.0.3     ✔ tibble    3.3.1
#> ✔ lubridate 1.9.5     ✔ tidyr     1.3.2
#> ✔ purrr     1.2.2
#> ── Conflicts ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── tidyverse_conflicts() ──
#> ✖ dplyr::filter() masks stats::filter()
#> ✖ dplyr::lag()    masks stats::lag()
#> ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
library(DBI)

# define the path and name of the database file
db_name <- "test_stock_db2"
db_path <- file.path(tempdir(), db_name)

# create the database connection
db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
```

## Check the database structure and contents

``` r
# check the connection
is_valid_db_connection(db_con)
```

\[1\] TRUE

``` r

# List tables in the database. For this example, we expect to see nothing as we have not populated it yet.
tables <- DBI::dbListTables(db_con)
tables
```

character(0)

``` r

# Clean all tables if any exist (for re-running the example)
purrr::walk(tables, ~DBI::dbRemoveTable(db_con, .x))
```
