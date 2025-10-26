test_that("is_valid_db_connection: connection is null", {
  con <- NULL
  expect_false(is_valid_db_connection(con))
})

test_that("is_valid_db_connection: connection is invalid", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
  DBI::dbDisconnect(con)
  expect_false(is_valid_db_connection(con))
})

# test_that("is_valid_db_connection: connection is not duckdb", {
#   con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#   expect_false(is_valid_db_connection(con))
#   DBI::dbDisconnect(con)
# })

test_that("is_valid_db_connection: valid duckdb connection", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
  expect_true(is_valid_db_connection(con))
  DBI::dbDisconnect(con)
})
