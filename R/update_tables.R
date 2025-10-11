#' Update the table with all symbols basic information
#'
#' This function updates the 'all_symbols' table in the database with the latest stock symbols and their basic information from various indexes.
#'
#' It fetches data from the ASX, B3, and S&P 500 indexes, combines them, and updates the database table accordingly.
#'
#' @param indexes A character vector specifying which indexes to filter by. Options include "ASX", "B3", "SP500", and "Benchmark". If NULL, returns all symbols.
#' @param db_con A DBI database connection object.
#' @return Nothing. The function updates the 'all_symbols' table in the database, informing the user of the update status.
#'
#' @examples
#' \dontrun{
#' # define the path and name of the database file
#' db_name <- "test_stock_db"
#' db_path <- file.path(tempdir(), db_name)
#' # create the database connection
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
#' # Update the symbols table
#' update_symbols_table(indexes = c("ASX", "B3"), db_con = db_con)
#' DBI::dbDisconnect(db_con)
#' }
#' @export
#'
update_symbols_table <- function(indexes = NULL, db_con, save_data = TRUE) {
  # Validate the database connection
  if (is.null(db_con) || !DBI::dbIsValid(db_con)) {
    stop("Invalid or NULL database connection provided.")
  }

  # If indexes parameter is provided, filter the data accordingly
  valid_indexes <- c("ASX", "B3", "SP500")
  if (!all(indexes %in% valid_indexes) | !is.null(indexes)) {
    stop(paste(
      "Invalid index provided. Valid options are:",
      paste(valid_indexes, collapse = ", ")
    ))
  }

  all_symbols <- tibble::tibble()

  # Fetch new symbols from various indexes
  if (is.null(indexes) | "ASX" %in% indexes) {
    asx_symbols <- scrap_asx_symbols()
    all_symbols <- dplyr::bind_rows(
      all_symbols,
      dplyr::mutate(asx_symbols, index = "ASX")
    )
  }
  if (is.null(indexes) | "B3" %in% indexes) {
    b3_symbols <- scrap_b3_symbols()
    all_symbols <- dplyr::bind_rows(
      all_symbols,
      dplyr::mutate(b3_symbols, index = "B3")
    )
  }
  if (is.null(indexes) | "SP500" %in% indexes) {
    sp500_symbols <- scrap_sp500_symbols()
    all_symbols <- dplyr::bind_rows(
      all_symbols,
      dplyr::mutate(sp500_symbols, index = "SP500")
    )
  }

  # If save_data is TRUE, save the data as an internal dataset
  if (save_data) {
    # Write the updated symbols back to the database
    DBI::dbWriteTable(db_con, "all_symbols", all_symbols, append = TRUE)
    message("The 'all_symbols' table has been updated successfully.")
    return(invisible(NULL))
  } else {
    return(all_symbols)
  }
}

#' Create benchmark symbols or add new ones if they don't exist
#'
#' This function creates a predefined set of benchmark symbols with their associated information.
#' If the benchmark symbols already exist in the database, it checks for any new symbols and adds them.
#' Despite having the same structure as 'all_symbols', the 'benchmark_symbols' are not expecting data changes, therefore, no updates are usually needed.
#'
#' @return Nothing. The function updates the 'benchmark_symbols' table in the database.
#' @examples
#' \dontrun{
#' # define the path and name of the database file
#' db_name <- "test_stock_db"
#' db_path <- file.path(tempdir(), db_name)
#' # create the database connection
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
#' # Create or update benchmark symbols
#' update_benchmarks(db_con = db_con)
#' DBI::dbDisconnect(db_con)
#' }
#' @export
#'
update_benchmarks <- function(db_con, new_benchmarks = NULL) {
  # Validate the database connection
  if (is.null(db_con) || !DBI::dbIsValid(db_con)) {
    stop("Invalid or NULL database connection provided.")
  }

  # Check if the benchmark symbols table already exists in the database
  table_exists <- DBI::dbExistsTable(db_con, "benchmark_symbols")
  if (table_exists & is.null(new_benchmarks)) {
    message(
      "The table benchmark_symbols already exists. Provide new_benchmarks to add new symbols."
    )
    return(invisible(NULL))
  }

  if (!table_exists) {
    # If the table does not exist, create it and insert all base benchmark symbols
    # Create the benchmark symbols data frame
    base_benchmarks <- create_benchmarks()

    DBI::dbWriteTable(db_con, "benchmark_symbols", base_benchmarks)
    message("Base Benchmark symbols table created and populated successfully.")
  }

  # If the table exists, check for new benchmark symbols to add
  if (!is.null(new_benchmarks)) {
    # Ensure new_benchmarks has the correct structure
    required_cols <- c("symbol", "name", "index", "source")

    if (!all(required_cols %in% colnames(new_benchmarks))) {
      stop(paste(
        "The new_benchmarks data frame must contain the following columns:",
        paste(required_cols, collapse = ", ")
      ))
    } # end if structure is correct

    # Read existing benchmark symbols from the database
    existing_benchmarks <- DBI::dbReadTable(db_con, "benchmark_symbols")

    # Identify new symbols that are not in the existing benchmarks
    new_benchmarks <- new_benchmarks %>%
      dplyr::filter(!symbol %in% existing_benchmarks$symbol)

    if (nrow(new_benchmarks) > 0) {
      # Append new benchmark symbols to the existing table
      DBI::dbWriteTable(
        db_con,
        "benchmark_symbols",
        new_benchmarks,
        append = TRUE
      )
      message(paste(
        nrow(new_benchmarks),
        "New benchmark symbols added to the database."
      ))
    } else {
      message(
        "The table benchmark_symbols already have the benchmarks you want"
      ) # end if new rows to add
    }
  } # end if new_benchmarks is not NULL

  return(invisible(NULL))
}
