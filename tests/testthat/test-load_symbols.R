# Create a temporary duckdb database with sample data for testing
temp_db <- tempfile(fileext = ".duckdb")
temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

test_that("load_all_symbols: the table all_symbols does not exist", {
  # ignore the error about the table not existing
  testthat::expect_error(
    load_all_symbols(temp_con),
    "Table 'all_symbols' does not exist in the database."
  )
})

test_that("load_all_symbols: the table benchmark_symbols does not exist", {
  # create an empty all_symbols table
  DBI::dbExecute(
    temp_con,
    "CREATE TABLE all_symbols (
      symbol TEXT,
      name TEXT,
      index TEXT,
      date_updated DATE
    )"
  )
  # ignore the error about the table not existing
  testthat::expect_error(
    load_all_symbols(temp_con),
    "Table 'benchmark_symbols' does not exist in the database."
  )
})

test_that("load_all_symbols: if the connection is bad, an error should be thrown", {
  bad_con <- "fake_connection"
  expect_error(
    load_all_symbols(bad_con),
    "Failed to connect to the database."
  )
})


test_that("load_all_symbols: if the connection is good you get the data", {
  # load sample data
  all_symbols <- sample_all_symbols
  all_benchmarks <- invest.data::create_benchmarks()
  # Insert the sample data into the database as the all_symbols table
  DBI::dbWriteTable(temp_con, "all_symbols", all_symbols, overwrite = TRUE)
  DBI::dbWriteTable(
    temp_con,
    "benchmark_symbols",
    all_benchmarks,
    overwrite = TRUE
  )
  # test that the function returns a list with 5 data frames
  result <- load_all_symbols(temp_con)
  expect_equal(class(result), "list")
  expect_equal(length(result), 5)
  expect_equal(class(result$all_symbols), "data.frame")
})

# Disconnect and remove the temporary database
DBI::dbDisconnect(temp_con, shutdown = TRUE)
