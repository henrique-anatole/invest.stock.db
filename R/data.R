#' Sample of stock symbols data
#'
#' A sample dataset containing stock symbols and related information for randomly selected companies.
#' Its used for testing and demonstration purposes.
#'
#'
"sample_all_symbols"


#' Sample of fundamental data
#'
#' A sample dataset containing fundamental financial data for "AAPL", "MSFT", "GOOGL".
#' Its used for testing and demonstration purposes.
#'
#'
"sample_fundamentals"

#' Sample of stock price data
#'
#' A sample dataset containing stock price data for "AAPL", "MSFT", "GOOGL".
#' Its used for testing and demonstration purposes.
#'
"sample_stock_prices"

# temp_db <- tempfile(fileext = ".duckdb")
# temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

# symbols_to_get <- c("AAPL", "MSFT", "GOOGL")
# start_date <- as.character(Sys.Date() - lubridate::days(365))
# interval <- "1d"
# # 1.1  Create the daily prices table in this new database
# update_stock_prices(
#   temp_con,
#   symbols_to_get,
#   start_date,
#   interval
# )

# start_date <- as.character(Sys.Date() - lubridate::days(60))
# interval <- "1h"
# # 1.2  Create the intraday prices table in this new database
# update_stock_prices(
#   temp_con,
#   symbols_to_get,
#   start_date,
#   interval
# )

# dataset <- list(
#   daily_prices = DBI::dbReadTable(temp_con, "daily_prices") %>%
#     dplyr::as_tibble(),
#   hourly_prices = DBI::dbReadTable(temp_con, "hourly_prices") %>%
#     dplyr::as_tibble()
# )

# saveRDS(dataset, file = "data/sample_stock_prices.rds")

# files_to_get <- c(
#   "balance_sheet_equity",
#   "balance_sheet_assets",
#   "balance_sheet_liabilities",
#   "cash_flow_statement",
#   "earnings_calendar",
#   "eps_estimate",
#   "eps_history",
#   "income_statement",
#   "rank_score",
#   "sales_estimate"
# )

# dataset <- list()

# for (table in files_to_get) {
#   # Optimization: Get only existing keys if possible.
#   # For fundamentals, we usually need the full row to ensure uniqueness.
#   db_existing <- DBI::dbReadTable(db_con, table) %>%
#     dplyr::as_tibble() %>%
#     dplyr::filter(act_symbol %in% c("AAPL", "MSFT", "GOOGL")) # Example symbols for testing

#   dataset[[table]] <- db_existing
# }

# saveRDS(dataset, file = "data/sample_fundamentals.rds")
