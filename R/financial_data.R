# library(invest.stock.db)
# # Create a temporary duckdb database with sample data for testing
# temp_db <- tempfile(fileext = ".duckdb")
# temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

# # Add each of the tables from the sample_stock_prices dataset to the temporary database for testing
# for (table_name in names(sample_stock_prices)) {
#   DBI::dbWriteTable(
#     temp_con,
#     table_name,
#     sample_stock_prices[[table_name]],
#     overwrite = TRUE
#   )
# }

# # Add each of the tables from the sample_fundamentals dataset to the temporary database for testing
# for (table_name in names(sample_fundamentals)) {
#   DBI::dbWriteTable(
#     temp_con,
#     table_name,
#     sample_fundamentals[[table_name]],
#     overwrite = TRUE
#   )
#   print(paste(table_name))
#   print(str(sample_fundamentals[[table_name]]))
# }

# benchmarks <- invest.data::create_benchmarks()
# DBI::dbWriteTable(
#   temp_con,
#   "benchmark_symbols",
#   benchmarks,
#   overwrite = TRUE
# )

# #tables in the database
# DBI::dbListTables(temp_con)

# # read the daily_prices table to use as input for the add_indicators function
# prepared_data <- DBI::dbReadTable(temp_con, "daily_prices")

# #create a fake daily price series for ^GSPC and insert into the database
# gspc_data <- data.frame(
#   symbol = "^GSPC",
#   open_time = seq(
#     min(prepared_data$open_time),
#     max(prepared_data$open_time),
#     by = "days"
#   ),
#   open = runif(
#     length(seq(
#       min(prepared_data$open_time),
#       max(prepared_data$open_time),
#       by = "days"
#     )),
#     3000,
#     4000
#   ),
#   high = runif(
#     length(seq(
#       min(prepared_data$open_time),
#       max(prepared_data$open_time),
#       by = "days"
#     )),
#     3000,
#     4000
#   ),
#   low = runif(
#     length(seq(
#       min(prepared_data$open_time),
#       max(prepared_data$open_time),
#       by = "days"
#     )),
#     3000,
#     4000
#   ),
#   close = runif(
#     length(seq(
#       min(prepared_data$open_time),
#       max(prepared_data$open_time),
#       by = "days"
#     )),
#     3000,
#     4000
#   ),
#   volume = NA
# )
# DBI::dbWriteTable(
#   temp_con,
#   "daily_prices",
#   gspc_data,
#   append = TRUE
# )
# # read the daily_prices table again to use as input for the add_indicators function, now with the ^GSPC data included
# prepared_data <- DBI::dbReadTable(temp_con, "daily_prices")

# test_earnings <- get_eps_data(
#   temp_con,
#   prepared_data
# )
# # str(test_earnings)

# db_con = temp_con
# symbol = NULL
# start_date = NULL
# end_date = NULL
# symbols_price_data = prepared_data
# period = "Quarter"

#' Get balance sheet equity data
#' @description Retrieves balance sheet equity data with period labels and end dates.
#' @param db_con A DBI connection object.
#' @param symbol A character vector of symbols to filter by.
#' @param start_date A character string representing the start date for filtering.
#' @param end_date A character string representing the end date for filtering.
#' @return A data frame with balance sheet equity information.
#'
#' @example
#'
#'
#' @export
get_balance_sheet_equity <- function(
  db_con,
  symbol = NULL,
  start_date = NULL,
  end_date = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }
  # Validate symbol
  if (!is.null(symbol)) {
    if (!is.character(symbol)) {
      symbol <- as.character(symbol)
    }
    if (length(symbol) == 0) {
      symbol <- NULL
    }
    if (any(is.na(symbol))) stop("symbol contains NA values.")
  }
  # Validate dates
  if (!is.null(start_date) && is.na(as.Date(start_date, "%Y-%m-%d"))) {
    stop("start_date must be a valid date string (YYYY-MM-DD).")
  }
  if (!is.null(end_date) && is.na(as.Date(end_date, "%Y-%m-%d"))) {
    stop("end_date must be a valid date string (YYYY-MM-DD).")
  }

  # Base SQL query
  sql_query <- "
    SELECT
      act_symbol as symbol,
      date as period_end_date,
      period,
      preferred_stock,
      common_stock,
      capital_surplus,
      retained_earnings,
      other_equity,
      treasury_stock,
      total_equity,
      total_liabilities_and_equity,
      shares_outstanding,
      book_value_per_share,
       -- Create period labels
          CASE
              WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
              WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
          END AS period_label
    FROM balance_sheet_equity
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND date <= '", end_date, "'")
  }
  if (!is.null(symbol)) {
    sql_query <- paste0(
      sql_query,
      " AND act_symbol IN ('",
      paste(symbol, collapse = "', '"),
      "')"
    )
  }
  sql_query <- paste0(sql_query, " ORDER BY act_symbol, date;")

  result <- tryCatch(
    {
      DBI::dbGetQuery(db_con, sql_query) %>% dplyr::distinct()
    },
    error = function(e) {
      data.frame(
        act_symbol = character(),
        period_end_date = as.Date(character()),
        period = character(),
        period_label = character(),
        preferred_stock = numeric(),
        common_stock = numeric(),
        capital_surplus = numeric(),
        retained_earnings = numeric(),
        other_equity = numeric(),
        treasury_stock = numeric(),
        total_equity = numeric(),
        total_liabilities_and_equity = numeric(),
        shares_outstanding = numeric(),
        book_value_per_share = numeric(),
        stringsAsFactors = FALSE
      )
    }
  )
  return(result)
}

#' Get income statement data
#' @description Retrieves income statement data with period labels and end dates.
#' @param db_con A DBI connection object.
#' @param symbol A character vector of symbols to filter by.
#' @param start_date A character string representing the start date for filtering.
#' @param end_date A character string representing the end date for filtering.
#' @return A data frame with income statement information.
#' @export
get_income_statement <- function(
  db_con,
  symbol = NULL,
  start_date = NULL,
  end_date = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }
  # Validate symbol
  if (!is.null(symbol)) {
    if (!is.character(symbol)) {
      symbol <- as.character(symbol)
    }
    if (length(symbol) == 0) {
      symbol <- NULL
    }
    if (any(is.na(symbol))) stop("symbol contains NA values.")
  }
  # Validate dates
  if (!is.null(start_date) && is.na(as.Date(start_date, "%Y-%m-%d"))) {
    stop("start_date must be a valid date string (YYYY-MM-DD).")
  }
  if (!is.null(end_date) && is.na(as.Date(end_date, "%Y-%m-%d"))) {
    stop("end_date must be a valid date string (YYYY-MM-DD).")
  }

  # Base SQL query
  sql_query <- "
    SELECT
      act_symbol as symbol,
      date as period_end_date,
      period,
      sales,
      cost_of_goods,
      gross_profit,
      selling_administrative_depreciation_amortization_expenses,
      income_after_depreciation_and_amortization,
      non_operating_income,
      interest_expense,
      pretax_income,
      income_taxes,
      minority_interest,
      investment_gains,
      other_income,
      income_from_continuing_operations,
      extras_and_discontinued_operations,
      net_income,
      income_before_depreciation_and_amortization,
      depreciation_and_amortization,
      average_shares,
      diluted_eps_before_non_recurring_items,
      diluted_net_eps,
       -- Create period labels
          CASE
              WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
              WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
          END AS period_label
    FROM income_statement
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND date <= '", end_date, "'")
  }
  if (!is.null(symbol)) {
    sql_query <- paste0(
      sql_query,
      " AND act_symbol IN ('",
      paste(symbol, collapse = "', '"),
      "')"
    )
  }
  sql_query <- paste0(sql_query, " ORDER BY act_symbol, date;")

  result <- tryCatch(
    {
      dbGetQuery(db_con, sql_query) %>% dplyr::distinct()
    },
    error = function(e) {
      data.frame(
        act_symbol = character(),
        date = as.Date(character()),
        period = character(),
        period_label = character(),
        sales = numeric(),
        cost_of_goods = numeric(),
        gross_profit = numeric(),
        selling_administrative_depreciation_amortization_expenses = numeric(),
        income_after_depreciation_and_amortization = numeric(),
        non_operating_income = numeric(),
        interest_expense = numeric(),
        pretax_income = numeric(),
        income_taxes = numeric(),
        minority_interest = numeric(),
        investment_gains = numeric(),
        other_income = numeric(),
        income_from_continuing_operations = numeric(),
        extras_and_discontinued_operations = numeric(),
        net_income = numeric(),
        income_before_depreciation_and_amortization = numeric(),
        depreciation_and_amortization = numeric(),
        average_shares = numeric(),
        diluted_eps_before_non_recurring_items = numeric(),
        diluted_net_eps = numeric(),
        stringsAsFactors = FALSE
      )
    }
  )
  return(result)
}

#' Get sales estimates
#' @description Retrieves sales estimates with period labels and end dates.
#' @param db_con A DBI connection object.
#' @param symbol A character vector of symbols to filter by.
#' @param start_date A character string representing the start date for filtering.
#' @param end_date A character string representing the end date for filtering.
#' @return A data frame with sales estimates information.
#' @export
get_sales_estimates <- function(
  db_con,
  symbol = NULL,
  start_date = NULL,
  end_date = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }
  # Validate symbol
  if (!is.null(symbol)) {
    if (!is.character(symbol)) {
      symbol <- as.character(symbol)
    }
    if (length(symbol) == 0) {
      symbol <- NULL
    }
    if (any(is.na(symbol))) stop("symbol contains NA values.")
  }
  # Validate dates
  if (!is.null(start_date) && is.na(as.Date(start_date, "%Y-%m-%d"))) {
    stop("start_date must be a valid date string (YYYY-MM-DD).")
  }
  if (!is.null(end_date) && is.na(as.Date(end_date, "%Y-%m-%d"))) {
    stop("end_date must be a valid date string (YYYY-MM-DD).")
  }

  # Base SQL query
  sql_query <- "
  WITH sales_estimate AS (  
    SELECT
      act_symbol as symbol,
      date as sales_estimates_date,
      period,
      period_end_date,
      consensus as sales_estimates_consensus,
      count as sales_estimates_count,
      high as sales_estimates_high,
      low as sales_estimates_low,
      year_ago as sales_estimates_year_ago,
        -- Create period labels
            CASE
                WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM period_end_date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM period_end_date - INTERVAL 63 DAY))
                WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM period_end_date - INTERVAL 63 DAY) AS VARCHAR)
            END AS period_label
    FROM sales_estimate
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND date <= '", end_date, "'")
  }
  if (!is.null(symbol)) {
    sql_query <- paste0(
      sql_query,
      " AND act_symbol IN ('",
      paste(symbol, collapse = "', '"),
      "')"
    )
  }

  # Continue the SQL query
  sql_query <- paste0(
    sql_query,
    "
    ),
    sales_estimate_comparison AS (
      SELECT
        *,
      -- get the previous period's consensus for comparison
        LAG(sales_estimates_consensus) OVER (PARTITION BY symbol, period_label ORDER BY sales_estimates_date) AS previous_consensus
      FROM sales_estimate
    )
    SELECT
      symbol,
      sales_estimates_date,
      -- Adjust open_time to the next day, since most estimates are released on weekends
      sales_estimates_date + INTERVAL '1 day' AS open_time,
      period,
      period_end_date,
      sales_estimates_consensus,
      sales_estimates_count,
      sales_estimates_high,
      sales_estimates_low,
      sales_estimates_year_ago,
      period_label,
      -- Create a label for whether the consensus is up, down, or unchanged compared to the previous period
      CASE
        WHEN sales_estimates_consensus > previous_consensus THEN TRUE
        ELSE FALSE
        END AS sales_estimates_grew,
      CASE
        WHEN sales_estimates_consensus < previous_consensus THEN TRUE
        ELSE FALSE
        END AS sales_estimates_declined,
      -- Calculate columns
      sales_estimates_consensus - previous_consensus AS sales_consensus_change,
      sales_consensus_change / NULLIF(ABS(previous_consensus), 0) * 100 AS sales_consensus_change_percent,
      sales_estimates_high - sales_estimates_low AS sales_consensus_range,
      sales_consensus_range / NULLIF(ABS(sales_estimates_consensus), 0) * 100 AS sales_dispersion_ratio
    FROM sales_estimate_comparison
    ORDER BY symbol, period_label, sales_estimates_date;"
  )

  # sql_query <- paste0(sql_query, " ORDER BY act_symbol, date;")

  result <- tryCatch(
    {
      dbGetQuery(db_con, sql_query) %>% dplyr::distinct()
    },
    error = function(e) {
      data.frame(
        symbol = character(),
        sales_estimates_date = as.Date(character()),
        period = character(),
        period_end_date = as.Date(character()),
        sales_estimates_consensus = numeric(),
        sales_estimates_count = integer(),
        sales_estimates_high = numeric(),
        sales_estimates_low = numeric(),
        sales_estimates_year_ago = numeric(),
        period_label = character(),
        sales_estimates_grew = logical(),
        sales_estimates_declined = logical(),
        stringsAsFactors = FALSE
      )
    }
  )

  sales_optimism <- result %>%
    # split period in two, creating a current_next column
    dplyr::mutate(
      current_next = ifelse(grepl("Current", period), "current", "next"),
      period = case_when(
        grepl("Quarter", period) ~ "Quarter",
        grepl("Year", period) ~ "Year",
        TRUE ~ period
      ),
    ) %>%
    dplyr::arrange(symbol, period_label, sales_estimates_date) %>%
    tidyr::pivot_wider(
      names_from = current_next,
      id_cols = c(symbol, sales_estimates_date, period),
      values_from = c(
        sales_estimates_consensus
      ),
      names_sep = "_"
    ) %>%
    dplyr::mutate(
      sales_consensus_optimist = case_when(
        current < `next` ~ TRUE,
        TRUE ~ FALSE
      ),
      sales_consensus_pessimist = case_when(
        current > `next` ~ TRUE,
        TRUE ~ FALSE
      )
    ) %>%
    dplyr::select(-c(current, `next`))

  ### next period will be excluded from the final result to keep data clean
  result_current <- result %>%
    # split period in two, creating a current_next column
    dplyr::mutate(
      current_next = ifelse(grepl("Current", period), "current", "next"),
      period = case_when(
        grepl("Quarter", period) ~ "Quarter",
        grepl("Year", period) ~ "Year",
        TRUE ~ period
      ),
    ) %>%
    dplyr::filter(current_next == "current") %>%
    dplyr::left_join(
      sales_optimism,
      by = c("symbol", "sales_estimates_date", "period")
    ) %>%
    dplyr::select(-current_next)

  return(result_current)
}

#' Get integrated company valuation data
#' @description Retrieves and merges balance sheet, income statement, and sales estimate data for given symbols and dates.
#' @param db_con A DBI connection object.
#' @param symbols_price_data A data frame containing at least a 'symbol' column (and optionally a date column).
#' @param start_date A character string representing the start date for filtering.
#' @param end_date A character string representing the end date for filtering.
#' @param period A character string indicating the period type for labeling (e.g., "Quarter" or "Year").
#' @return A data frame with merged valuation data.
#' @export
get_valuation_data <- function(
  db_con,
  symbols_price_data,
  start_date = NULL,
  end_date = NULL,
  period = "Quarter"
) {
  # 1. Validation
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }
  if (!("symbol" %in% names(symbols_price_data))) {
    stop("symbols_price_data must contain a 'symbol' column.")
  }
  symbol <- unique(symbols_price_data$symbol)
  if (length(symbol) == 0) {
    return(symbols_price_data[0, , drop = FALSE])
  }

  period_filter <- dplyr::case_when(
    period == "Quarter" ~ "Quarter",
    period == "Year" ~ "Year"
  )

  # 2. Retrieve data
  # Get earnings calendar data to merge with balance sheet data
  earnings_calendar <- get_earnings_calendar(db_con)

  balance_sheet <- tryCatch(
    {
      get_balance_sheet_equity(db_con, symbol, start_date, end_date) %>%
        filter(grepl(period_filter, period)) %>%
        left_join(
          earnings_calendar,
          by = c("symbol", "period_label", "period_end_date")
        ) %>%
        # Replace NA in date columns with period_end_date + 1 month (assuming the earnings release happens after the period end date)
        mutate(
          date = if_else(
            is.na(date),
            period_end_date + lubridate::days(30),
            date
          ),
          is_balance_day = TRUE
        )
    },
    error = function(e) tibble::tibble()
  )

  income_statement <- tryCatch(
    {
      get_income_statement(db_con, symbol, start_date, end_date) %>%
        filter(grepl(period_filter, period))
    },
    error = function(e) tibble::tibble()
  )

  sales_estimates <- tryCatch(
    {
      get_sales_estimates(db_con, symbol, start_date, end_date) %>%
        filter(grepl(period_filter, period)) %>%
        arrange(symbol, period_label, sales_estimates_date)
    },
    error = function(e) tibble::tibble()
  )

  # 3. Merge data
  merged_data <- balance_sheet %>%
    left_join(
      income_statement,
      by = c("symbol", "period_end_date", "period", "period_label")
    ) %>%
    mutate(
      open_time = dplyr::case_when(
        before_open ~ date, # AM earnings affect same day
        after_close ~ date + lubridate::days(1), # PM earnings affect next day
        TRUE ~ date + lubridate::days(1) # Default to conservative
      )
    )

  # 4. Compare estimates with actuals and create labels for whether the consensus was optimistic or pessimistic compared to the actual sales
  sales_estimates_result <- sales_estimates %>%
    group_by(symbol, period_label, period_end_date) %>%
    arrange(sales_estimates_date) %>%
    reframe(
      sales_estimates_date = last(sales_estimates_date),
      sales_estimates_consensus = last(sales_estimates_consensus),
      sales_estimates_count = last(sales_estimates_count),
      sales_estimates_grew = last(sales_estimates_grew),
      sales_estimates_declined = last(sales_estimates_declined),
      sales_consensus_change = last(sales_consensus_change),
      sales_consensus_change_percent = last(sales_consensus_change_percent),
      sales_consensus_range = last(sales_consensus_range),
      sales_dispersion_ratio = last(sales_dispersion_ratio),
      sales_consensus_optimist = last(sales_consensus_optimist),
      sales_consensus_pessimist = last(sales_consensus_pessimist)
    )

  merged_data <- merged_data %>%
    left_join(
      sales_estimates_result,
      by = c("symbol", "period_label")
    ) %>%
    mutate(
      sales_surprise_amount = sales - sales_estimates_consensus,
      sales_surprise_percent = sales_surprise_amount / sales * 100
    ) %>%
    select(
      symbol,
      period_label,
      sales,
      sales_estimates_consensus,
      sales_surprise_amount,
      sales_surprise_percent
    )

  return(list(
    balance_sheet = balance_sheet,
    income_statement = income_statement,
    sales_estimates = sales_estimates,
    merged_data = merged_data
  ))
}

# symbols <- c("AAPL", "MSFT", "GOOGL")
#     splits_data <- purrr::map_df(
#       symbols,
#       ~ {
#         result <- tidyquant::tq_get(
#           .x,
#           get = "splits",
#           from = "1900-01-01",
#           to = Sys.Date()
#         )
#         if (nrow(result) == 0 || all(is.na(result)) || is.null(result)) {
#           message("No splits found for symbol: ", .x)
#           return(NULL)
#         } else {
#           return(result)
#         }
#       }
#     )
#     if (nrow(splits_data) == 0) {
#       splits_data <- NULL
#     }
