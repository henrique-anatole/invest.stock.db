#' Function to get all stock symbols from the DuckDB database
#'
#' This function connects to a DuckDB database, retrieves all stock symbols,
#' and filters them based on their respective indices (ASX, B3, SP500, Benchmark).
#'
#' @param db_con A valid DBI connection object to the DuckDB database.
#'
#' @return A list containing data frames for all symbols and filtered symbols by index.
#'
#' @import DBI
#' @import duckdb
#' @import dplyr
#' @import invest.data
#' @export
#' @examples
#' \dontrun{
#' db_file <- "/mnt/nas_nuvens/stock_data/stock_data - Copia.duckdb"
#' # create a connection to the database
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_file, read_only = TRUE)
#' symbols <- load_symbols(db_con)
#' head(symbols$all_symbols_au)
#' head(symbols$all_symbols_br)
#' head(symbols$all_symbols_sp500)
#' head(symbols$all_benchmarks)
#' dbDisconnect(db_con)
#' }
#'
load_symbols <- function(db_con) {
  # # check the connection
  if (!grepl("connection", class(db_con))) {
    stop("Failed to connect to the database.")
  }

  # Check if the table 'all_symbols' exists
  if (!"all_symbols" %in% DBI::dbListTables(db_con)) {
    stop("Table 'all_symbols' does not exist in the database.")
  }
  # query the database to check if the data was written
  all_symbol_data = dbGetQuery(db_con, "SELECT * FROM all_symbols") %>%
    filter(symbol != "-")

  # Check if data was retrieved
  if (nrow(all_symbol_data) == 0) {
    stop("No data found in the 'all_symbols' table.")
  }

  # # get the symbols from AUS
  all_symbols_au <- all_symbol_data %>%
    filter(index == "ASX")

  # # get the symbols from BRA
  all_symbols_br <- all_symbol_data %>%
    filter(index == "B3")

  # Get the rest of the data for USA symbols
  all_symbols_sp500 <- all_symbol_data %>%
    filter(index == "SP500")

  # Get benchmarks for comparisons
  all_benchmarks <- all_symbol_data %>%
    filter(index == "Benchmark")

  # disconnect from the database
  dbDisconnect(db_con)

  return(list(
    all_symbol_data = all_symbol_data,
    all_symbols_au = all_symbols_au,
    all_symbols_br = all_symbols_br,
    all_symbols_sp500 = all_symbols_sp500,
    all_benchmarks = all_benchmarks
  ))
}
