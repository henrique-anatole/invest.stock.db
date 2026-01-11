# Create a temporary duckdb database with sample data for testing
temp_db <- tempfile(fileext = ".duckdb")
temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

symbols_to_get <- c("A8G.AX", "AAPL", "MSFT")
start_date <- as.character(Sys.Date() - lubridate::days(200))
interval <- "1d"

testthat::test_that("update_stock_prices: Invalid connection", {
  # Create a bad connection object
  bad_con <- NULL
  # Test that the function throws an error for invalid connection
  testthat::expect_error(
    update_stock_prices(
      bad_con,
      symbols_to_get,
      start_date,
      interval
    ),
    "Invalid or NULL database connection provided."
  )
})

testthat::test_that("update_stock_prices: Invalid interval", {
  # Create a bad connection object
  bad_interval <- "5m"
  # Test that the function throws an error for invalid connection
  testthat::expect_error(
    update_stock_prices(
      temp_con,
      symbols_to_get,
      start_date,
      bad_interval
    ),
    "Invalid interval provided."
  )
})

testthat::test_that("update_stock_prices: Invalid start_date", {
  # Create a bad connection object
  bad_start_date <- "2023-13-01"
  # Test that the function throws an error for invalid connection
  testthat::expect_error(
    update_stock_prices(
      temp_con,
      symbols_to_get,
      bad_start_date,
      interval
    ),
    "Invalid start_date provided."
  )
})

testthat::test_that("update_stock_prices: Missing symbols_to_get in a new database", {
  # Test that the function throws an error for invalid connection
  testthat::expect_error(
    update_stock_prices(
      temp_con,
      NULL,
      start_date,
      interval
    ),
    regexp = "No symbols provided or failed to load"
  )
})

testthat::test_that("update_stock_prices: Working examples", {
  # 1 First use: Ensure the daily_prices table does not exist
  if ("daily_prices" %in% DBI::dbListTables(temp_con)) {
    DBI::dbExecute(temp_con, "DROP TABLE daily_prices")
  }
  interval <- "1d"
  # 1.1  Create the daily prices table in this new database
  update_stock_prices(
    temp_con,
    symbols_to_get,
    start_date,
    interval
  )

  # 1.2 Expect the daily_prices table to exist now, and have all symbols loaded
  testthat::expect_true("daily_prices" %in% DBI::dbListTables(temp_con))
  loaded_data <- DBI::dbGetQuery(temp_con, "SELECT * FROM daily_prices")
  testthat::expect_equal(unique(loaded_data$symbol), symbols_to_get)

  summary_daily <- DBI::dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol"
  )

  # 2 Updating the existing table with new data
  # 2.1 If the database is updated, message should indicate no new data is needed
  expect_message(
    update_stock_prices(
      temp_con,
      symbols_to_get,
      start_date,
      interval
    ),
    "No new rows to insert."
  )

  # 2.1 Fake the data to be old
  fake_old_date <- loaded_data %>%
    filter(open_time < as.character(Sys.Date() - 30))
  # 2.2 write back the fake old data to the database
  DBI::dbWriteTable(temp_con, "daily_prices", fake_old_date, overwrite = TRUE)
  # 2.3 Check the contents of the daily_prices table
  symbols_before <- dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol"
  )
  ## Now run the update function again to get the missing recent data
  symbols_to_get_2 <- c("A8G.AX", "AAPL", "MSFT", "GOOGL")
  update_stock_prices(
    temp_con,
    symbols_to_get_2,
    start_date,
    interval
  )
  ## Check the table contents after updating
  symbols_after <- dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol"
  )
  testthat::expect_equal(
    sort(unique(symbols_after$symbol)),
    sort(symbols_to_get_2)
  )

  # remove the table created and disconnect
  DBI::dbRemoveTable(temp_con, "daily_prices")
})

testthat::test_that("update_stock_prices: Working examples for hourly", {
  # 1 First use: Ensure the hourly_prices table does not exist
  if ("hourly_prices" %in% DBI::dbListTables(temp_con)) {
    DBI::dbExecute(temp_con, "DROP TABLE hourly_prices")
  }
  interval <- "1h"

  symbols_to_get <- c("A8G.AX", "AAPL", "MSFT")

  # 1.1  Create the hourly prices table in this new database
  test <- update_stock_prices(
    temp_con,
    symbols_to_get,
    start_date,
    interval
  )

  # 1.2 Expect the hourly_prices table to exist now, and have all symbols loaded
  testthat::expect_true("hourly_prices" %in% DBI::dbListTables(temp_con))
  loaded_data <- DBI::dbGetQuery(temp_con, "SELECT * FROM hourly_prices")
  symbols_loaded <- unique(loaded_data$symbol) %>% sort()
  # Check that all requested symbols are loaded, except those that may not have hourly data (unique(test$errors$symbol))
  testthat::expect_equal(
    unique(loaded_data$symbol),
    symbols_to_get[!(symbols_to_get %in% unique(test$errors$symbol))] %>% sort()
  )

  summary_hourly <- DBI::dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM hourly_prices GROUP BY symbol"
  )

  # 2 Updating the existing table with new data
  # 2.1 If the database is updated, message should indicate no new data is needed
  expect_warning(
    update <- update_stock_prices(
      temp_con,
      symbols_to_get,
      start_date,
      interval
    ),
    "tickers failed to download"
  )

  # 2.1 Fake the data to be old
  fake_old_date <- loaded_data %>%
    filter(open_time < as.character(Sys.Date() - 30))
  # 2.2 write back the fake old data to the database
  DBI::dbWriteTable(temp_con, "hourly_prices", fake_old_date, overwrite = TRUE)
  # 2.3 Check the contents of the hourly_prices table
  symbols_before <- dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM hourly_prices GROUP BY symbol"
  )
  ## Now run the update function again to get the missing recent data
  symbols_to_get_2 <- c("A8G.AX", "AAPL", "MSFT", "GOOGL")
  new_update <- update_stock_prices(
    temp_con,
    symbols_to_get_2,
    start_date,
    interval
  )
  ## Check the table contents after updating
  symbols_after <- dbGetQuery(
    temp_con,
    "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM hourly_prices GROUP BY symbol"
  )
  testthat::expect_equal(
    sort(unique(symbols_after$symbol)),
    sort(symbols_to_get_2[
      !(symbols_to_get_2 %in% unique(new_update$errors$symbol))
    ])
  )

  # remove the table created and disconnect
  DBI::dbRemoveTable(temp_con, "hourly_prices")
})


# Disconnect and remove the temporary database
DBI::dbDisconnect(temp_con, shutdown = TRUE)
