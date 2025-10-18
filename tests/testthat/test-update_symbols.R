test_that("update_benchmarks: benchmark_table first run", {
  # define the path and name of the database file
  db_name <- "test_stock_db"
  db_path <- file.path(tempdir(), db_name)
  # create the database connection
  db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)

  # First run: table does not exist yet
  expect_message(
    update_benchmarks(db_con = db_con),
    regexp = "Base Benchmark symbols table created and populated successfully."
  )

  # Check that the table now exists
  expect_true(DBI::dbExistsTable(db_con, "benchmark_symbols"))

  # Check the contents of the table
  benchmarks <- DBI::dbReadTable(db_con, "benchmark_symbols")
  expect_true(nrow(benchmarks) > 0)
  # No duplicate symbols
  expect_equal(nrow(benchmarks), length(unique(benchmarks$symbol)))

  # delete the table created
  DBI::dbRemoveTable(db_con, "benchmark_symbols")

  DBI::dbDisconnect(db_con)
})

test_that("update_benchmarks: null conection", {
  expect_error(
    update_benchmarks(db_con = NULL),
    regexp = "Invalid or NULL database connection provided."
  )
})

test_that("update_benchmarks: new benchmarks", {
  # define the path and name of the database file
  db_name <- "test_stock_db"
  db_path <- file.path(tempdir(), db_name)
  # create the database connection
  db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)

  # First run: table does not exist yet
  update_benchmarks(db_con = db_con)

  # Try to run it again without new benchmarks
  expect_message(
    update_benchmarks(db_con = db_con),
    regexp = "The table benchmark_symbols already exists. Provide new_benchmarks to add new symbols."
  )

  # test it with new benchmarks
  new_benchmarks <- data.frame(
    symbol = c("^NEW1", "^NEW2"),
    name = c("New Benchmark 1", "New Benchmark 2"),
    index = c("New Index 1", "New Index 2"),
    source = c("Yahoo Finance", "Yahoo Finance"),
    stringsAsFactors = FALSE
  )

  expect_message(
    update_benchmarks(db_con = db_con, new_benchmarks = new_benchmarks),
    regexp = "New benchmark symbols added to the database."
  )

  # Missing columns
  new_bad_benchmarks <- data.frame(
    symbol = c("^NEW1", "^NEW2"),
    index = c("New Index 1", "New Index 2"),
    source = c("Yahoo Finance", "Yahoo Finance"),
    stringsAsFactors = FALSE
  )

  expect_error(
    update_benchmarks(db_con = db_con, new_benchmarks = new_bad_benchmarks),
    regexp = "The new_benchmarks data frame must contain the following columns:"
  )

  # Check that the new benchmarks were added
  updated_benchmarks <- DBI::dbReadTable(db_con, "benchmark_symbols")
  expect_true(all(new_benchmarks$symbol %in% updated_benchmarks$symbol))

  # Try to add the same new benchmarks again
  expect_message(
    update_benchmarks(db_con = db_con, new_benchmarks = new_benchmarks),
    regexp = "The table benchmark_symbols already have the benchmarks you want"
  )
  # Check that no duplicates were added
  final_benchmarks <- DBI::dbReadTable(db_con, "benchmark_symbols")
  expect_equal(nrow(updated_benchmarks), nrow(final_benchmarks))

  # delete the table created
  DBI::dbRemoveTable(db_con, "benchmark_symbols")

  DBI::dbDisconnect(db_con)
})

test_that("update_symbols_table: first run", {
  # define the path and name of the database file
  db_name <- "test_stock_db"
  db_path <- file.path(tempdir(), db_name)
  # create the database connection
  db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)

  # First run: table does not exist yet
  expect_message(
    update_symbols_table(db_con = db_con),
    regexp = "The 'all_symbols' table has been updated successfully"
  )

  # Check that the table now exists
  expect_true(DBI::dbExistsTable(db_con, "all_symbols"))

  # Check the contents of the table
  all_symbols <- DBI::dbReadTable(db_con, "all_symbols")

  # expect symbols from all sources
  expect_true(all(unique(all_symbols$index) %in% c("SP500", "ASX", "B3")))

  # delete the table created
  DBI::dbRemoveTable(db_con, "all_symbols")

  DBI::dbDisconnect(db_con)
})

test_that("update_symbols_table: null conection", {
  expect_error(
    update_symbols_table(db_con = NULL),
    regexp = "Invalid or NULL database connection provided."
  )
})

test_that("update_symbols_table: dont save", {
  # define the path and name of the database file
  db_name <- "test_stock_db"
  db_path <- file.path(tempdir(), db_name)
  # create the database connection
  db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)

  # First run: table does not exist yet
  all_symbols = update_symbols_table(db_con = db_con, save_data = FALSE)

  # Check that the table now does not exist
  expect_false(DBI::dbExistsTable(db_con, "all_symbols"))

  # Check the contents of the returned data frame
  expect_true(nrow(all_symbols) > 0)

  # All sources are present
  expect_true(all(unique(all_symbols$index) %in% c("SP500", "ASX", "B3")))

  DBI::dbDisconnect(db_con)
})

test_that("update_symbols_table: invalid index", {
  # define the path and name of the database file
  db_name <- "test_stock_db"
  db_path <- file.path(tempdir(), db_name)
  # create the database connection
  db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)

  expect_error(
    update_symbols_table(db_con = db_con, indexes = c("INVALID_INDEX")),
    regexp = "Invalid index provided. Valid options are:"
  )

  DBI::dbDisconnect(db_con)
})
