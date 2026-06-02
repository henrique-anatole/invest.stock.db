library(DBI)
# Setup: create a temporary duckdb database with sample data
setup_test_db <- function() {
  temp_db <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

  # Load sample data
  data("sample_stock_prices", package = "invest.stock.db")
  data("sample_fundamentals", package = "invest.stock.db")

  # Write stock price tables

  for (table_name in names(sample_stock_prices)) {
    DBI::dbWriteTable(
      con,
      table_name,
      sample_stock_prices[[table_name]],
      overwrite = TRUE
    )
  }

  # Write fundamental tables
  for (table_name in names(sample_fundamentals)) {
    DBI::dbWriteTable(
      con,
      table_name,
      sample_fundamentals[[table_name]],
      overwrite = TRUE
    )
  }

  # check all tables are written
  tables <- DBI::dbListTables(con)

  con
}

teardown_test_db <- function(con) {
  DBI::dbDisconnect(con)
}

# --- get_balance_sheet_equity tests ---

testthat::test_that("get_balance_sheet_equity: returns data for all symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_balance_sheet_equity(con)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("symbol" %in% names(result))
  testthat::expect_true("period_end_date" %in% names(result))
  testthat::expect_true("period_label" %in% names(result))
  testthat::expect_true("total_equity" %in% names(result))
  testthat::expect_true("book_value_per_share" %in% names(result))
})

testthat::test_that("get_balance_sheet_equity: filters by symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_balance_sheet_equity(con, symbol = "AAPL")

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$symbol == "AAPL"))
})

testthat::test_that("get_balance_sheet_equity: filters by multiple symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_balance_sheet_equity(con, symbol = c("AAPL", "MSFT"))

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$symbol %in% c("AAPL", "MSFT")))
})

testthat::test_that("get_balance_sheet_equity: filters by date range", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_balance_sheet_equity(
    con,
    start_date = "2020-01-01",
    end_date = "2022-12-31"
  )

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$period_end_date >= as.Date("2020-01-01")))
  testthat::expect_true(all(result$period_end_date <= as.Date("2022-12-31")))
})

testthat::test_that("get_balance_sheet_equity: errors on invalid connection", {
  testthat::expect_error(
    get_balance_sheet_equity(NULL),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_balance_sheet_equity: errors on NA symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  testthat::expect_error(
    get_balance_sheet_equity(con, symbol = NA_character_),
    "symbol contains NA values"
  )
})

testthat::test_that("get_balance_sheet_equity: errors on invalid date format", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  testthat::expect_error(
    get_balance_sheet_equity(con, start_date = "not-a-date"),
    "start_date must be a valid date string"
  )
  testthat::expect_error(
    get_balance_sheet_equity(con, end_date = "not-a-date"),
    "end_date must be a valid date string"
  )
})

testthat::test_that("get_balance_sheet_equity: returns empty data frame for nonexistent symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_balance_sheet_equity(con, symbol = "ZZZZZ")

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 0)
})

# --- get_income_statement tests ---

testthat::test_that("get_income_statement: returns data for all symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_income_statement(con)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("symbol" %in% names(result))
  testthat::expect_true("period_end_date" %in% names(result))
  testthat::expect_true("period_label" %in% names(result))
  testthat::expect_true("net_income" %in% names(result))
  testthat::expect_true("sales" %in% names(result))
})

testthat::test_that("get_income_statement: filters by symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_income_statement(con, symbol = "MSFT")

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$symbol == "MSFT"))
})

testthat::test_that("get_income_statement: filters by date range", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_income_statement(
    con,
    start_date = "2020-01-01",
    end_date = "2023-12-31"
  )

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$period_end_date >= as.Date("2020-01-01")))
  testthat::expect_true(all(result$period_end_date <= as.Date("2023-12-31")))
})

testthat::test_that("get_income_statement: errors on invalid connection", {
  testthat::expect_error(
    get_income_statement(NULL),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_income_statement: errors on NA symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  testthat::expect_error(
    get_income_statement(con, symbol = NA_character_),
    "symbol contains NA values"
  )
})

testthat::test_that("get_income_statement: returns empty data frame for nonexistent symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_income_statement(con, symbol = "ZZZZZ")

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 0)
})

# --- get_sales_estimates tests ---

testthat::test_that("get_sales_estimates: returns data for all symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_sales_estimates(con)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("symbol" %in% names(result))
  testthat::expect_true("sales_estimates_consensus" %in% names(result))
  testthat::expect_true("period_label" %in% names(result))
})

testthat::test_that("get_sales_estimates: filters by symbol", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_sales_estimates(con, symbol = "GOOGL")

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(result$symbol == "GOOGL"))
})

testthat::test_that("get_sales_estimates: filters by date range", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_sales_estimates(
    con,
    start_date = "2021-01-01",
    end_date = "2023-12-31"
  )

  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true(all(
    result$sales_estimates_date >= as.Date("2021-01-01")
  ))
  testthat::expect_true(all(
    result$sales_estimates_date <= as.Date("2023-12-31")
  ))
})

testthat::test_that("get_sales_estimates: errors on invalid connection", {
  testthat::expect_error(
    get_sales_estimates(NULL),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_sales_estimates: contains optimism/pessimism columns", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_sales_estimates(con, symbol = "AAPL")

  testthat::expect_true("sales_consensus_optimist" %in% names(result))
  testthat::expect_true("sales_consensus_pessimist" %in% names(result))
})

# --- get_valuation_data tests ---

testthat::test_that("get_valuation_data: returns enriched data frame with valid inputs", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_valuation_data(con, price_data)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  # Required balance sheet columns
  testthat::expect_true("common_stock" %in% names(result))
  testthat::expect_true("retained_earnings" %in% names(result))
  testthat::expect_true("total_equity" %in% names(result))
  testthat::expect_true("shares_outstanding" %in% names(result))
  testthat::expect_true("book_value_per_share" %in% names(result))
  # Required income statement columns
  testthat::expect_true("sales" %in% names(result))
  testthat::expect_true("average_shares" %in% names(result))
  # Valuation ratios
  testthat::expect_true("market_cap" %in% names(result))
  testthat::expect_true("price_to_book" %in% names(result))
  testthat::expect_true("price_to_sales" %in% names(result))
  testthat::expect_true("price_to_earnings" %in% names(result))
  testthat::expect_true("earnings_yield" %in% names(result))
  testthat::expect_true("return_on_equity" %in% names(result))
  testthat::expect_true("enterprise_value_proxy" %in% names(result))
  testthat::expect_true("ev_to_sales" %in% names(result))
})

testthat::test_that("get_valuation_data: has same number of rows as input price data", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_valuation_data(con, price_data)

  testthat::expect_equal(nrow(result), nrow(price_data))
})

testthat::test_that("get_valuation_data: errors on invalid connection", {
  price_data <- data.frame(symbol = "AAPL")

  testthat::expect_error(
    get_valuation_data(NULL, price_data),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_valuation_data: errors if symbols_price_data missing symbol column", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  bad_data <- data.frame(ticker = "AAPL")

  testthat::expect_error(
    get_valuation_data(con, bad_data),
    "symbols_price_data must contain a 'symbol' column"
  )
})

testthat::test_that("get_valuation_data: returns early for empty symbol list", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  empty_data <- data.frame(symbol = character(0))

  result <- get_valuation_data(con, empty_data)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 0)
})

testthat::test_that("get_valuation_data: forward-fills fundamental values", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_valuation_data(con, price_data)

  # After forward-filling, most rows should have non-NA valuation data
  # (except rows before the first fundamental release)
  aapl <- result[result$symbol == "AAPL", ]
  # Find first fundamental release
  first_release <- min(which(aapl$is_fundamental_release))
  if (!is.infinite(first_release) && first_release < nrow(aapl)) {
    # After the first release, book_value_per_share should be filled
    testthat::expect_true(
      !all(is.na(aapl$book_value_per_share[(first_release + 1):nrow(aapl)]))
    )
  }
})

testthat::test_that("get_valuation_data: filters by period Year", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_valuation_data(con, price_data, period = "Year")

  testthat::expect_s3_class(result, "data.frame")
  # Fundamental release rows should only be from Year periods
  releases <- result[result$is_fundamental_release, ]
  if (nrow(releases) > 0) {
    testthat::expect_true(all(grepl("Year", releases$period)))
  }
})

# --- get_leverage_data tests ---

testthat::test_that("get_leverage_data: returns enriched data frame", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_leverage_data(con, price_data)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("total_debt" %in% names(result))
  testthat::expect_true("net_debt" %in% names(result))
  testthat::expect_true("debt_to_equity" %in% names(result))
  testthat::expect_true("current_ratio" %in% names(result))
  testthat::expect_true("quick_ratio" %in% names(result))
  testthat::expect_true("enterprise_value" %in% names(result))
  testthat::expect_true("ev_to_ebitda" %in% names(result))
  testthat::expect_true("net_debt_to_ebitda" %in% names(result))
})

testthat::test_that("get_leverage_data: has same rows as input", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_leverage_data(con, price_data)

  testthat::expect_equal(nrow(result), nrow(price_data))
})

testthat::test_that("get_leverage_data: errors on invalid connection", {
  price_data <- data.frame(symbol = "AAPL")

  testthat::expect_error(
    get_leverage_data(NULL, price_data),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_leverage_data: errors if missing symbol column", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  testthat::expect_error(
    get_leverage_data(con, data.frame(ticker = "AAPL")),
    "symbols_price_data must contain a 'symbol' column"
  )
})

testthat::test_that("get_leverage_data: returns early for empty symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_leverage_data(con, data.frame(symbol = character(0)))

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 0)
})

# --- get_cash_flow_data tests ---

testthat::test_that("get_cash_flow_data: returns enriched data frame", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_cash_flow_data(con, price_data)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("free_cash_flow" %in% names(result))
  testthat::expect_true("price_to_fcf" %in% names(result))
  testthat::expect_true("fcf_yield" %in% names(result))
  testthat::expect_true("operating_cf_margin" %in% names(result))
  testthat::expect_true("capex_to_operating_cf" %in% names(result))
  testthat::expect_true("cf_quality" %in% names(result))
  testthat::expect_true("fcf_per_share" %in% names(result))
  testthat::expect_true("net_cash_from_operating_activities" %in% names(result))
  testthat::expect_true("capital_expenditures" %in% names(result))
})

testthat::test_that("get_cash_flow_data: has same rows as input", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_cash_flow_data(con, price_data)

  testthat::expect_equal(nrow(result), nrow(price_data))
})

testthat::test_that("get_cash_flow_data: errors on invalid connection", {
  price_data <- data.frame(symbol = "AAPL")

  testthat::expect_error(
    get_cash_flow_data(NULL, price_data),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("get_cash_flow_data: errors if missing symbol column", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  testthat::expect_error(
    get_cash_flow_data(con, data.frame(ticker = "AAPL")),
    "symbols_price_data must contain a 'symbol' column"
  )
})

testthat::test_that("get_cash_flow_data: returns early for empty symbols", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  result <- get_cash_flow_data(con, data.frame(symbol = character(0)))

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 0)
})

# --- get_full_fundamentals tests ---

testthat::test_that("get_full_fundamentals: combines all three functions", {
  con <- setup_test_db()
  withr::defer(teardown_test_db(con))

  price_data <- DBI::dbReadTable(con, "daily_prices")

  result <- get_full_fundamentals(con, price_data)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), nrow(price_data))
  # Valuation columns
  testthat::expect_true("price_to_book" %in% names(result))
  testthat::expect_true("price_to_earnings" %in% names(result))
  # Leverage columns
  testthat::expect_true("debt_to_equity" %in% names(result))
  testthat::expect_true("enterprise_value" %in% names(result))
  # Cash flow columns
  testthat::expect_true("free_cash_flow" %in% names(result))
  testthat::expect_true("fcf_yield" %in% names(result))
})

testthat::test_that("get_full_fundamentals: errors on invalid connection", {
  price_data <- data.frame(symbol = "AAPL")

  testthat::expect_error(
    get_full_fundamentals(NULL, price_data),
    "db_con must be a valid DBI connection"
  )
})
