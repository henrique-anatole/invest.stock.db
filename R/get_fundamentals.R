# Get financial data
get_financials <- function() {
  options(timeout = 300)

  file_path <- function(file_name) {
    paste0(
      "https://www.dolthub.com/csv/post-no-preference/earnings/master/",
      file_name,
      "?include_bom=0"
    )
  }

  # echo of the csv files named as in the vector to a different variable here
  files <- c(
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
  )

  # create the variables with the content of the csv files
  for (f in files) {
    # read the csv file into a variable with the same name as the file
    assign(f, read_csv(file_path(f)))

    # write all those tables to the database
    dbWriteTable(con, f, get(f), overwrite = TRUE)
    # remove the table from the environment
    rm(list = f)
  }
}

# delete all data after 2025-06-30 from the newly created variable
if ("date" %in% colnames(get(f))) {
  f_fake <- get(f) %>%
    filter(date <= "2025-06-30")
}
# get an f_new, which is the data in f which is not in f_fake
f_new <- anti_join(get(f), f_fake)
