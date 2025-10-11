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
  # skip_on_cran() # Cran does not allow webscraping tests
  # skip_if_offline() # optional, provided by testthat

  df <- scrap_sp500_symbols()
  expect_s3_class(df, "tbl_df")
  expect_true(all(c("symbol", "name", "sector", "market_cap") %in% names(df)))
  # Check that market_cap is numeric and non-negative
  expect_true(is.numeric(df$market_cap))
  expect_true(all(df$market_cap > 0, na.rm = TRUE))
})

test_that("scrap_asx_symbols: returns tibble with expected columns", {
  # skip_on_cran() # Cran does not allow webscraping tests
  # skip_if_offline() # optional, provided by testthat

  df <- scrap_asx_symbols()
  expect_s3_class(df, "tbl_df")
  expect_true(all(c("symbol", "name", "sector", "market_cap") %in% names(df)))
  # Check that market_cap is numeric and non-negative
  expect_true(is.numeric(df$market_cap))
  expect_true(all(df$market_cap > 0, na.rm = TRUE))
})

test_that("scrap_asx_symbols: error scrapping page", {
  # skip_on_cran() # Cran does not allow webscraping tests
  # skip_if_offline() # optional, provided by testthat

  # Simulate a failure in reading the webpage by providing an invalid URL
  # make read_html throw an error. For that, any call of rvest::read_html should return the fake_page object. Mock it
  with_mocked_bindings(
    read_html = function(...) fake_page,
    {
      expect_warning(
        scrap_asx_symbols(),
        regexp = "Error while scraping ASX data"
      )
    },
    .package = "rvest"
  )
})

test_that("scrap_b3_symbols: returns tibble with expected columns", {
  # skip_on_cran() # Cran does not allow webscraping tests
  # skip_if_offline() # optional, provided by testthat

  # Provide a local classification CSV for testing
  suppressMessages(
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
  # Creates a fake file
  temp_file <- tempfile(fileext = ".csv")
  writeLines("invalid,data,without,proper,columns", temp_file)
  expect_error(
    scrap_b3_symbols(class_file = temp_file),
    regexp = "Classification CSV must contain columns:"
  )
  unlink(temp_file) # Clean up
  # Creates a fake .txt file
  temp_file_txt <- tempfile(fileext = ".txt")
  writeLines("invalid data without proper columns", temp_file_txt)
  expect_error(
    scrap_b3_symbols(class_file = temp_file_txt),
    regexp = "Classification file must be a CSV"
  )
  unlink(temp_file_txt) # Clean up
})

test_that("scrap_b3_symbols: works with a valid temporary classification file", {
  # Creates a true temporary classification CSV file
  temp_valid_file <- tempfile(fileext = ".csv")
  write.csv(
    data.frame(
      symbol = c("TEST3", "TEST4"),
      company_name = c("Test Company 3", "Test Company 4"),
      sector = c("Sector 3", "Sector 4"),
      subsector = c("Subsector 3", "Subsector 4"),
      segment = c("Segment 3", "Segment 4"),
      stringsAsFactors = FALSE
    ),
    temp_valid_file,
    row.names = FALSE
  )
  suppressMessages(
    df <- scrap_b3_symbols()
  )
  expect_s3_class(df, "tbl_df")

  unlink(temp_valid_file) # Clean up
})

test_that("scrap_b3_symbols: error scrapping page", {
  # mock the navigate method of the RSelenium remote driver to simulate a failure
  fake_driver <- list(
    client = list(
      navigate = function(url) stop("Simulated navigation error"),
      findElement = function(...) NULL,
      close = function() NULL
    ),
    server = list(stop = function() NULL)
  )

  # Simulate a failure in reading the webpage by providing an invalid URL
  with_mocked_bindings(
    rsDriver = function(...) fake_driver,
    {
      expect_warning(
        scrap_b3_symbols(),
        regexp = "Error while scraping B3 data"
      )
    }
  )
})

test_that("scrap_b3_symbols: no file to download", {
  # mock the filepath so no file is found

  # Simulate a failure in reading the webpage by providing an invalid URL

  local_mocked_bindings(
    difftime = function(...) 10,
    .package = "base"
  )

  expect_warning(
    scrap_b3_symbols(),
    regexp = "Timeout waiting for B3 CSV download"
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
