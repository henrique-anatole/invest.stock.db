#' Update the daily prices table in the database
#'
#' This function updates the 'daily_prices' table in the database with the latest stock prices for all symbols selected.
#' It uses the `invest.data` package to fetch the data and updates the database accordingly.
#'
#' @param db_con A DBI database connection object.
#' @param symbols_to_get A character vector of stock symbols to update. If NULL, updates all symbols in the 'all_symbols' table.
#' @param start_date A character string representing the start date for fetching data (format: "YYYY-MM-DD").
#' @param interval A character string representing the data interval (e.g., "1d" for daily).
#'
#' @import invest.data
#' @import DBI
#' @import duckdb
#' @import dplyr
#'
#' @example path.R
#' \dontrun{
#' db_file <- "/mnt/nas_nuvens/stock_data/stock_data - Copia.duckdb"
#' # create a connection to the database
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_file, read_only = FALSE)
#' # Check the table contents before updating
#' symbols_to_get <- c("AAPL", "MSFT")
#' # Current data in the database
#' symbols_before <- dbGetQuery(db_con, "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol")
#' print(symbols_before)
#' #' # Update stock prices
#' update_stock_prices(db_con, symbols_to_get = c("AAPL", "MSFT"), start_date = "2020-01-01", interval = "1d")
#' # Check the table contents after updating
#' symbols_after <- dbGetQuery(db_con, "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol")
#' print(symbols_after)
#' dbDisconnect(db_con)
#' }
#' @export
update_stock_prices <- function(
  db_con,
  symbols_to_get = NULL,
  start_date = "2014-01-01",
  interval = "1d"
) {
  # Validate the database connection
  if (is.null(db_con) || !DBI::dbIsValid(db_con)) {
    stop("Invalid or NULL database connection provided.")
  }

  # Load symbols_to_get from the database if not provided
  if (is.null(symbols_to_get)) {
    symbols_df <- load_all_symbols(db_con)$all_symbol_data
    symbols_to_get <- unique(symbols_df$symbol)
  }

  # stop if the interval is not valid
  valid_intervals <- c("1d", "1h")
  if (!(interval %in% valid_intervals)) {
    stop(paste0(
      "Invalid interval provided. Valid intervals are: ",
      paste(valid_intervals, collapse = ", ")
    ))
  }

  # Check if start_date is valid
  invest.data::is_valid_date(start_date)
  symbol_type <- "stock" # "stock"
  end_date <- as.character(Sys.Date())

  # check the data already in the database to avoid duplications
  db_table <- case_when(
    interval == "1d" ~ "daily_prices",
    interval == "1h" ~ "hourly_prices"
  )

  # Does the table required exist?
  if (eval(parse(text = paste0("db_table %in% DBI::dbListTables(db_con)")))) {
    # Check the data available in the database
    db_table_summary <- DBI::dbGetQuery(
      db_con,
      paste0(
        "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM ",
        db_table,
        " GROUP BY symbol"
      )
    )

    # Which symbols_to_get are missing in the database?
    missing_symbols <- symbols_to_get %>%
      as.data.frame() %>%
      rename(symbol = ".") %>%
      anti_join(db_table_summary, by = "symbol") %>%
      pull(symbol)

    prices_data <- NULL
    prices_data_error <- NULL

    for (symbol in missing_symbols) {
      print(paste0("Getting data for missing symbol: ", symbol))

      # If any symbols are missing, get their data
      price_data <- invest.data::load_stock_timeseries(
        symbols = symbol,
        interval = interval,
        start_date = start_date,
        end_date = end_date
      )

      prices_data <- bind_rows(prices_data, price_data$data) %>%
        distinct()
      prices_data_error <- bind_rows(prices_data_error, price_data$errors) %>%
        distinct()
    }

    # check if there is duplicated data in prices_data
    prices_data <- prices_data %>%
      distinct()
    # check if any data is already in the database to avoid duplications
    daily_prices <- dbGetQuery(db_con, "SELECT * FROM daily_prices")
    # Remove any overlapping data to avoid duplication
    prices_data_rev <- prices_data %>%
      anti_join(
        daily_prices,
        by = c("symbol", "open_time", "close", "high", "low", "open", "volume")
      ) %>%
      distinct()

    # Add the new lines to the table "daily_prices" in the database
    dbWriteTable(db_con, "daily_prices", prices_data_rev, append = TRUE)

    # update the db_table_summary after adding missing symbols
    db_table_summary <- DBI::dbGetQuery(
      db_con,
      "SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol"
    )

    # any symbol still missing?
    still_missing_symbols <- symbols_to_get %>%
      as.data.frame() %>%
      rename(symbol = ".") %>%
      anti_join(db_table_summary, by = "symbol") %>%
      pull(symbol)

    if (length(still_missing_symbols) > 0) {
      warning(paste0(
        "The following symbols are still missing in the database after attempting to fetch their data: ",
        paste(still_missing_symbols, collapse = ", ")
      ))
    }

    # Update data for all other symbols
    other_symbols <- symbols_to_get[!symbols_to_get %in% missing_symbols]

    # Loop through each symbol and update data
    prices_data <- NULL
    prices_data_error <- NULL

    for (symbol in other_symbols) {
      print(paste0("Updating data for symbol: ", symbol))

      # Get the last date for the symbol from db_table_summary
      last_date <- db_table_summary %>%
        filter(symbol == !!symbol) %>%
        pull(last_date)

      # Fetch new data from the day after the last date to today
      price_data <- invest.data::load_stock_timeseries(
        symbols = symbol,
        interval = interval,
        start_date = as.character(last_date + 1),
        end_date = end_date
      )

      prices_data <- bind_rows(prices_data, price_data$data)
      prices_data_error <- bind_rows(prices_data_error, price_data$errors)
    }

    # Load existing daily prices from the database
    daily_prices <- dbGetQuery(db_con, "SELECT * FROM daily_prices")
    # Remove any overlapping data to avoid duplication
    prices_data_rev <- prices_data %>%
      anti_join(
        daily_prices,
        by = c("symbol", "open_time", "close", "high", "low", "open", "volume")
      ) %>%
      distinct()

    # Add the new lines to the table "daily_prices" in the database
    dbWriteTable(db_con, db_table, prices_data_rev, append = TRUE)
  }

  # Which symbols_to_get need to be updated in the database?
}
