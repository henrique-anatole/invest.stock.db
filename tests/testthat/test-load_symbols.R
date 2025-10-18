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
