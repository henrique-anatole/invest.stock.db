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

  # check if the benchmark symbols are already in the database
  benchmark_symbols <- tryCatch(
    {
      benchmark_symbols <- DBI::dbReadTable(db_con, "all_symbols")
      return(NULL)
    },
    error = function(e) {
      message(
        "Benchmark symbols not found in the database. Creating benchmark symbols."
      )
      return(create_benchmarks())
    }
  )

  all_symbols <- dplyr::bind_rows(all_symbols, benchmark_symbols)

  # Remove duplicates based on the 'symbol' column
  all_symbols <- all_symbols[!duplicated(all_symbols$symbol), ]

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
