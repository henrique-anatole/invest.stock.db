# files_to_get = c(
#   "balance_sheet_equity",
#   "balance_sheet_assets"
# )
# start_date = "2024-01-01"

#' Update financial statement tables in the database
#'
#' This function fetches the latest financial data and performs an incremental
#' update to the database. It ensures data consistency by using transactions
#' and anti-joins to prevent duplicate records.
#'
#' @param db_con A valid DBI database connection object.
#' @param files_to_get Character vector of financial table names to update.
#' @param start_date A character string "YYYY-MM-DD". If provided, filters
#'   fresh data before comparing with the database.
#'
#' @return A list (invisible) containing a summary of rows added per table and any errors.
#' @importFrom DBI dbIsValid dbListTables dbGetQuery dbWriteTable dbBegin dbCommit dbRollback
#' @importFrom dplyr distinct anti_join filter bind_rows tibble as_tibble
#' @importFrom purrr map
#' @export
#'
update_fundamentals <- function(
  db_con,
  files_to_get = c(
    "balance_sheet_equity",
    "balance_sheet_assets",
    "balance_sheet_liabilities",
    "cash_flow_statement",
    "earnings_calendar",
    "eps_estimate",
    "eps_history",
    "income_statement",
    "rank_score",
    "sales_estimate"
  ),
  start_date = NULL
) {
  # ---- 1. Validation ----
  # Validate the database connection
  if (!is_valid_db_connection(db_con)) {
    stop("Invalid or NULL database connection provided.")
  }
  # Validate start_date format if provided
  if (!is.null(start_date) && !invest.data::is_valid_date(start_date)) {
    stop("Invalid start_date provided. Must be in 'YYYY-MM-DD' format.")
  }

  # ---- 2. Fetch Fresh Data ----
  message("Fetching financial datasets from source...")
  # Standardized call to the retrieval function
  fundamentals <- invest.data::get_fundamentals_data(
    files_to_get = files_to_get,
    timeout = 300
  )

  if (is.null(fundamentals$data) || length(fundamentals$data) == 0) {
    message("No data retrieved from source. Update aborted.")
    return(invisible(list(summary = NULL, errors = fundamentals$errors)))
  }

  # Only process files requested that were actually downloaded
  available_files <- intersect(names(fundamentals$data), files_to_get)
  update_log <- dplyr::tibble(table = character(), rows_added = numeric())

  # ---- 3. Database Transaction ----
  DBI::dbBegin(db_con)

  dataset <- available_files[1] # Initialize for error handling context

  tryCatch(
    {
      for (dataset in available_files) {
        message("Processing dataset: ", dataset)

        data_loaded <- fundamentals$data[[dataset]]

        # Optional Date Filtering
        if (!is.null(start_date) && "date" %in% colnames(data_loaded)) {
          data_loaded <- data_loaded %>%
            dplyr::filter(as.Date(date) >= as.Date(start_date))
        }

        # Check for existing table to handle anti-join
        if (dataset %in% DBI::dbListTables(db_con)) {
          # Optimization: Get only existing keys if possible.
          # For fundamentals, we usually need the full row to ensure uniqueness.
          db_existing <- DBI::dbReadTable(db_con, dataset) %>%
            dplyr::as_tibble() %>%
            dplyr::filter(
              if (!is.null(start_date) && "date" %in% colnames(.)) {
                as.Date(date) >= as.Date(start_date)
              } else {
                TRUE
              }
            )

          # Perform anti-join to find new records
          new_data_final <- data_loaded %>%
            dplyr::anti_join(
              db_existing,
              by = intersect(colnames(data_loaded), colnames(db_existing))
            ) %>%
            dplyr::distinct()
        } else {
          # Table doesn't exist, all loaded data is new
          new_data_final <- dplyr::distinct(data_loaded)
        }

        # Write data
        rows_to_add <- nrow(new_data_final)
        if (rows_to_add > 0) {
          DBI::dbWriteTable(
            db_con,
            dataset,
            new_data_final,
            append = TRUE,
            row.names = FALSE
          )
        }

        update_log <- dplyr::bind_rows(
          update_log,
          dplyr::tibble(table = dataset, rows_added = rows_to_add)
        )
        message("Updated ", dataset, ": +", rows_to_add, " records.")
      }

      DBI::dbCommit(db_con)
      message("Database transaction committed successfully.")
    },
    error = function(e) {
      DBI::dbRollback(db_con)
      stop(
        "Transaction rolled back due to error in table '",
        dataset,
        "': ",
        e$message
      )
    }
  )

  # ---- 4. Return Summary ----
  if (!is.null(fundamentals$errors)) {
    warning(
      "Some files failed to download: ",
      paste(fundamentals$errors$file, collapse = ", ")
    )
  }

  invisible(list(
    summary = update_log,
    errors = fundamentals$errors
  ))
}
