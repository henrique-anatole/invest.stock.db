# Create a temporary duckdb database with sample data for testing
temp_db <- tempfile(fileext = ".duckdb")
temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

test_that("load_symbols: the table all_symbols does not exist", {
  # ignore the error about the table not existing
  testthat::expect_error(
    load_symbols(temp_con),
    "Table 'all_symbols' does not exist in the database."
  )
})

test_that("load_symbols: if the connection is bad, an error should be thrown", {
  bad_con <- "fake_connection"
  expect_error(
    load_symbols(bad_con),
    "Failed to connect to the database."
  )
})

test_that("load_symbols: if the table exists but is empty, an error should be thrown", {
  # create an empty data frame with the same structure as the all_symbols table
  all_symbols <- data.frame(
    symbol = character(0),
    stringsAsFactors = FALSE
  )
  # Insert the empty data frame into the database as the all_symbols table
  DBI::dbWriteTable(temp_con, "all_symbols", all_symbols, overwrite = TRUE)
  # test that the error is thrown
  expect_error(
    load_symbols(temp_con),
    "No data found in the 'all_symbols' table."
  )
  # remove the table
  DBI::dbRemoveTable(temp_con, "all_symbols")
})

test_that("load_symbols: if the connection is good you get the data", {
  # load sample data
  all_symbols <- sample_all_symbols
  # Insert the sample data into the database as the all_symbols table
  DBI::dbWriteTable(temp_con, "all_symbols", all_symbols, overwrite = TRUE)
  # test that the function returns a list with 5 data frames
  result <- load_symbols(temp_con)
  expect_equal(class(result), "list")
  expect_equal(length(result), 5)
  expect_equal(class(result$all_symbol_data), "data.frame")
})

test_that("scrap_sp500_symbols: returns tibble with expected columns", {
  # skip_if_offline()
  df <- scrap_sp500_symbols()
  expect_s3_class(df, "tbl_df")
  expect_true(all(c("symbol", "name", "sector", "market_cap") %in% names(df)))
  # Check that market_cap is numeric and non-negative
  expect_true(is.numeric(df$market_cap))
  expect_true(all(df$market_cap > 0, na.rm = TRUE))
})

test_that("scrap_asx_symbols: returns tibble with expected columns", {
  # skip_if_offline()
  df <- scrap_asx_symbols()
  expect_s3_class(df, "tbl_df")
  expect_true(all(c("symbol", "name", "sector", "market_cap") %in% names(df)))
  # Check that market_cap is numeric and non-negative
  expect_true(is.numeric(df$market_cap))
  expect_true(all(df$market_cap > 0, na.rm = TRUE))
})

test_that("scrap_b3_symbols: returns tibble with expected columns", {
  skip_on_cran()

  # Provide a local classification CSV for testing
  suppressWarnings(
    df <- scrap_b3_symbols()
  )
  expect_s3_class(df, "tbl_df")

  expected_cols <- c(
    "symbol",
    "name",
    "sector",
    "subsector",
    "market_cap",
    "estimated_tot_shares",
    "price",
    "coin",
    "weight",
    "date_updated",
    "source",
    "index",
    "rank"
  )

  expect_true(all(expected_cols %in% names(df)))

  # market_cap and estimated_tot_shares numeric and non-negative
  expect_true(is.numeric(df$market_cap))
  expect_true(all(df$market_cap >= 0, na.rm = TRUE))
  expect_true(is.numeric(df$estimated_tot_shares))
  expect_true(all(df$estimated_tot_shares >= 0, na.rm = TRUE))
})

test_that("scrap_b3_symbols: handles missing classification file gracefully", {
  # Provide a wrong path
  expect_error(
    scrap_b3_symbols(class_file = "nonexistent_file.csv"),
    regexp = "Classification file not found at the provided path"
  )
})

test_that("create_benchmarks: check if returns the expected data frame", {
  res <- create_benchmarks()
  expect_s3_class(res, "data.frame")
  expected_cols <- c(
    "symbol",
    "name",
    "index",
    "date_updated",
    "source"
  )
  expect_true(all(expected_cols %in% names(res)))
})
