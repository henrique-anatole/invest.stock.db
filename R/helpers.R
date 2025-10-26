#' Check is you have a valid database connection
#'
#' @param db_con A valid DBI database connection object.
#'
#' @return TRUE if the connection is valid, FALSE otherwise.
#' @importFrom DBI dbIsValid
#' @examples
#' \dontrun{
#' # Create a database connection
#' db_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
#' # Check if the connection is valid
#' is_valid_db_connection(db_con)
#' # Disconnect from the database
#' DBI::dbDisconnect(db_con)
#' }
#' @export
#'
is_valid_db_connection <- function(db_con) {
  if (is.null(db_con) || !DBI::dbIsValid(db_con)) {
    return(FALSE)
  }
  if (!grepl("duckdb", class(db_con))) {
    return(FALSE)
  }
  return(TRUE)
}
