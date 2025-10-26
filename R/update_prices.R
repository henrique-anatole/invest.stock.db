#' Update the daily or hourly prices table in the database
#'
#' This function updates the 'daily_prices' or 'hourly_prices' table
#' in a DuckDB database with the latest stock prices for the selected symbols.
#'
#' It uses the `invest.data` package to fetch new or missing data, while
#' ensuring no duplication and maintaining data consistency.
#'
#' @param db_con A valid DBI database connection object.
#' @param symbols_to_get A character vector of stock symbols to update.
#'   If NULL, all symbols from `load_all_symbols(db_con)$all_symbol_data` are used.
#' @param start_date A character string "YYYY-MM-DD" (default: "2014-01-01").
#' @param interval A character string, either "1d" or "1h" (default: "1d").
#'
#' @importFrom DBI dbIsValid dbListTables dbGetQuery dbWriteTable dbBegin dbCommit dbRollback
#' @importFrom dplyr distinct anti_join rename filter pull case_when bind_rows
#' @importFrom invest.data load_stock_timeseries is_valid_date
#'
#' @examples
#' \dontrun{
#' # 1 Creating the first table
#' ## define the path and name of the database file
#' db_name <- "test_stock_db"
#' db_path <- file.path(tempdir(), db_name)
#' ## create the database connection
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
#' ## Get the important variables
#' symbols_to_get <- c("A8G.AX", "AAPL", "MSFT")
#' start_date <- as.character(Sys.Date() - days(200))
#' interval <- "1d"
#' ## Create the daily prices table in this new database
#' update_stock_prices(
#' db_con,
#' symbols_to_get,
#' start_date,
#' interval
#' )
#' ## Check the contents of the daily_prices table
#' loaded_data <- DBI::dbGetQuery(db_con, "SELECT * FROM daily_prices")
#' # 2 Updating the existing table with new data
#' ## Fake the data to be old
#' fake_old_date <- loaded_data %>% filter(open_time < as.character(Sys.Date() - 30))
#' ## write back the fake old data to the database
#' DBI::dbWriteTable(db_con, "daily_prices", fake_old_date, overwrite = TRUE)
#' ## Check the contents of the daily_prices table
#' symbols_before <- dbGetQuery(db_con,"SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol")
#' ## Now run the update function again to get the missing recent data
#' symbols_to_get <- c("A8G.AX", "AAPL", "MSFT", "GOOGL")
#' update_stock_prices(db_con, symbols_to_get, start_date = "2020-01-01", interval = "1d")
#' ## Check the table contents after updating
#' symbols_after <- dbGetQuery(db_con,"SELECT symbol, MIN(open_time) as first_date, MAX(open_time) as last_date FROM daily_prices GROUP BY symbol")
#'
#' DBI::dbDisconnect(db_con)
#' }
#' @export
#'
update_stock_prices <- function(
  db_con,
  symbols_to_get = NULL,
  start_date = "2014-01-01",
  interval = "1d"
) {
  # ---- Validation ----
  if (!is_valid_db_connection(db_con)) {
    stop("Invalid or NULL database connection provided.")
  }

  valid_intervals <- c("1d", "1h")
  if (!(interval %in% valid_intervals)) {
    stop(sprintf(
      "Invalid interval provided. Must be one of: %s",
      paste(valid_intervals, collapse = ", ")
    ))
  }

  if (!invest.data::is_valid_date(start_date)) {
    stop("Invalid start_date provided. Must be in 'YYYY-MM-DD' format.")
  }

  # ---- Load symbols ----
  if (is.null(symbols_to_get)) {
    symbols_df <- load_all_symbols(db_con)$all_symbol_data
    symbols_to_get <- unique(symbols_df$symbol)
  }

  if (length(symbols_to_get) == 0) {
    stop("No symbols provided or found in database.")
  }

  db_table <- dplyr::case_when(
    interval == "1d" ~ "daily_prices",
    interval == "1h" ~ "hourly_prices"
  )

  # ---- Check database state ----
  if (!(db_table %in% DBI::dbListTables(db_con))) {
    # Create a summary from the data available
    db_table_summary <- dplyr::tibble(
      symbol = character(0),
      first_date = as.Date(character(0)),
      last_date = as.Date(character(0))
    )
  } else {
    db_table_summary <- DBI::dbGetQuery(
      db_con,
      sprintf(
        "SELECT symbol, MIN(open_time) AS first_date, MAX(open_time) AS last_date FROM %s GROUP BY symbol",
        db_table
      )
    )
  }
  # ---- Split symbols ----
  missing_symbols <- setdiff(symbols_to_get, db_table_summary$symbol)
  existing_symbols <- setdiff(symbols_to_get, missing_symbols)
  end_date <- as.character(Sys.Date())

  # Prepare containers
  all_new_data <- dplyr::tibble()
  all_errors <- dplyr::tibble()

  DBI::dbBegin(db_con)
  tryCatch(
    {
      # ---- 1. Fetch missing symbols ----
      if (length(missing_symbols) > 0) {
        for (symbol in missing_symbols) {
          price_data <- invest.data::load_stock_timeseries(
            symbols = symbol,
            interval = interval,
            start_date = start_date,
            end_date = end_date
          )

          if (!is.null(price_data$data) && nrow(price_data$data) > 0) {
            all_new_data <- dplyr::bind_rows(all_new_data, price_data$data)
          }
          if (!is.null(price_data$errors) && nrow(price_data$errors) > 0) {
            all_errors <- dplyr::bind_rows(all_errors, price_data$errors)
          }
        }
      }

      # ---- 2. Update existing symbols ----
      if (length(existing_symbols) > 0) {
        for (symbol in existing_symbols) {
          last_date <- db_table_summary %>%
            dplyr::filter(symbol == !!symbol) %>%
            dplyr::pull(last_date)

          if (length(last_date) == 0 || is.na(last_date)) {
            next
          }

          start_update <- as.character(as.Date(last_date) + 1)
          if (start_update > end_date) {
            next
          }

          price_data <- invest.data::load_stock_timeseries(
            symbols = symbol,
            interval = interval,
            start_date = start_update,
            end_date = end_date
          )

          if (!is.null(price_data$data) && nrow(price_data$data) > 0) {
            all_new_data <- dplyr::bind_rows(all_new_data, price_data$data)
          }
          if (!is.null(price_data$errors) && nrow(price_data$errors) > 0) {
            all_errors <- dplyr::bind_rows(all_errors, price_data$errors)
          }
        }
      }

      # ---- 3. Remove duplicates vs DB ----
      if (nrow(all_new_data) > 0) {
        all_new_data <- dplyr::distinct(all_new_data)
        unique_symbols <- unique(all_new_data$symbol)
        symbol_list_sql <- paste0(
          "'",
          paste(unique_symbols, collapse = "','"),
          "'"
        )

        # Only load existing data for relevant symbols
        # Check if the table exists before querying
        if (!(db_table %in% DBI::dbListTables(db_con))) {
          DBI::dbWriteTable(db_con, db_table, all_new_data)
        } else {
          existing_data <- DBI::dbGetQuery(
            db_con,
            sprintf(
              "SELECT symbol, open_time, close, high, low, open, volume FROM %s WHERE symbol IN (%s)",
              db_table,
              symbol_list_sql
            )
          )

          new_data_final <- all_new_data %>%
            dplyr::anti_join(
              existing_data,
              by = c(
                "symbol",
                "open_time",
                "close",
                "high",
                "low",
                "open",
                "volume"
              )
            ) %>%
            dplyr::distinct()

          if (nrow(new_data_final) > 0) {
            DBI::dbWriteTable(db_con, db_table, new_data_final, append = TRUE)
            message(nrow(new_data_final), " new rows appended to ", db_table)
          } else {
            message("No new rows to insert.")
          }
        }
      } else {
        message("No new data retrieved.")
      }

      DBI::dbCommit(db_con)
    },
    error = function(e) {
      DBI::dbRollback(db_con)
      stop("Transaction rolled back due to error: ", e$message)
    }
  )

  if (nrow(all_errors) > 0) {
    warning(
      "Some symbols returned errors during fetch:\n",
      paste(unique(all_errors$symbol), collapse = ", ")
    )
  }

  invisible(list(new_rows = nrow(all_new_data), errors = all_errors))
}
