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
#' @description Retrieves balance sheet equity, income statement, and sales estimate data,
#' merges them with daily price data, forward-fills fundamentals, and computes valuation ratios
#' (P/B, P/S, P/E, market cap, enterprise value proxies) for each trading day.
#'
#' @details
#' **Use cases:**
#' - Value screening: rank stocks by P/B, P/E, earnings yield
#' - Momentum + value factor models: combine with price-based indicators
#' - Earnings surprise analysis: detect post-announcement drift using sales surprise columns
#' - Sector-relative valuation: compare P/S or EV/Sales across peers
#'
#' **Complementary functions:**
#' - \code{\link{get_leverage_data}}: adds debt/equity, current ratio, net debt, proper EV, EV/EBITDA
#' - \code{\link{get_cash_flow_data}}: adds FCF, P/FCF, FCF yield, operating CF margin
#' - \code{\link{get_full_fundamentals}}: convenience wrapper combining all three
#'
#' @param db_con A DBI connection object.
#' @param symbols_price_data A data frame containing at least 'symbol', 'open_time', and 'close' columns.
#' @param start_date A character string representing the start date for filtering (YYYY-MM-DD).
#' @param end_date A character string representing the end date for filtering (YYYY-MM-DD).
#' @param period A character string indicating the period type ("Quarter" or "Year").
#' @return A data frame with daily price rows enriched with forward-filled fundamental
#' and valuation columns.
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

  # 2. Retrieve component data
  earnings_calendar <- tryCatch(
    get_earnings_calendar(db_con),
    error = function(e) {
      tibble::tibble(
        symbol = character(),
        date = as.Date(character()),
        period_label = character(),
        period_end_date = as.Date(character())
      )
    }
  )

  balance_sheet <- tryCatch(
    {
      get_balance_sheet_equity(db_con, symbol, start_date, end_date) %>%
        dplyr::filter(grepl(period_filter, period)) %>%
        dplyr::left_join(
          earnings_calendar,
          by = c("symbol", "period_label", "period_end_date")
        ) %>%
        dplyr::mutate(
          date = dplyr::if_else(
            is.na(date),
            period_end_date + lubridate::days(30),
            date
          )
        )
    },
    error = function(e) tibble::tibble()
  )

  income_statement <- tryCatch(
    {
      get_income_statement(db_con, symbol, start_date, end_date) %>%
        dplyr::filter(grepl(period_filter, period))
    },
    error = function(e) tibble::tibble()
  )

  sales_estimates <- tryCatch(
    {
      get_sales_estimates(db_con, symbol, start_date, end_date) %>%
        dplyr::filter(grepl(period_filter, period)) %>%
        dplyr::arrange(symbol, period_label, sales_estimates_date)
    },
    error = function(e) tibble::tibble()
  )

  # 3. Merge fundamentals into a single snapshot per period
  fundamentals <- balance_sheet %>%
    dplyr::left_join(
      income_statement %>%
        dplyr::select(
          symbol,
          period_end_date,
          period,
          period_label,
          sales,
          cost_of_goods,
          gross_profit,
          net_income,
          average_shares,
          diluted_eps_before_non_recurring_items,
          diluted_net_eps
        ),
      by = c("symbol", "period_end_date", "period", "period_label")
    ) %>%
    dplyr::mutate(
      # Determine when the market first sees the data
      open_time = dplyr::case_when(
        !is.na(before_open) & before_open ~ date,
        !is.na(after_close) & after_close ~ date + lubridate::days(1),
        TRUE ~ date + lubridate::days(1)
      ),
      is_fundamental_release = TRUE
    ) %>%
    dplyr::select(
      symbol,
      open_time,
      period_label,
      period_end_date,
      period,
      is_fundamental_release,
      # Balance sheet equity
      common_stock,
      retained_earnings,
      other_equity,
      treasury_stock,
      total_equity,
      total_liabilities_and_equity,
      shares_outstanding,
      book_value_per_share,
      # Income statement
      sales,
      cost_of_goods,
      gross_profit,
      net_income,
      average_shares,
      diluted_eps_before_non_recurring_items,
      diluted_net_eps
    )

  # 4. Get last sales estimate per period for surprise calculations
  sales_estimates_last <- sales_estimates %>%
    dplyr::group_by(symbol, period_label, period_end_date) %>%
    dplyr::arrange(sales_estimates_date) %>%
    dplyr::reframe(
      sales_estimates_consensus = dplyr::last(sales_estimates_consensus),
      sales_estimates_count = dplyr::last(sales_estimates_count),
      sales_estimates_grew = dplyr::last(sales_estimates_grew),
      sales_estimates_declined = dplyr::last(sales_estimates_declined),
      sales_consensus_change = dplyr::last(sales_consensus_change),
      sales_consensus_change_percent = dplyr::last(
        sales_consensus_change_percent
      ),
      sales_consensus_range = dplyr::last(sales_consensus_range),
      sales_dispersion_ratio = dplyr::last(sales_dispersion_ratio),
      sales_consensus_optimist = dplyr::last(sales_consensus_optimist),
      sales_consensus_pessimist = dplyr::last(sales_consensus_pessimist)
    )

  # Add sales estimate data to fundamentals
  fundamentals <- fundamentals %>%
    dplyr::left_join(
      sales_estimates_last,
      by = c("symbol", "period_label", "period_end_date")
    ) %>%
    dplyr::mutate(
      sales_surprise_amount = sales - sales_estimates_consensus,
      sales_surprise_percent = dplyr::if_else(
        abs(sales) < 1e-10 | is.na(sales),
        NA_real_,
        sales_surprise_amount / abs(sales) * 100
      )
    )

  # 5. Join fundamentals to price data and forward-fill
  result <- symbols_price_data %>%
    dplyr::left_join(fundamentals, by = c("symbol", "open_time")) %>%
    dplyr::group_by(symbol) %>%
    dplyr::arrange(symbol, open_time) %>%
    dplyr::mutate(
      is_fundamental_release = dplyr::if_else(
        is.na(is_fundamental_release),
        FALSE,
        TRUE
      ),
      # Forward-fill fundamental values
      dplyr::across(
        c(
          period_label,
          period_end_date,
          period,
          common_stock,
          retained_earnings,
          other_equity,
          treasury_stock,
          total_equity,
          total_liabilities_and_equity,
          shares_outstanding,
          book_value_per_share,
          sales,
          cost_of_goods,
          gross_profit,
          net_income,
          average_shares,
          diluted_eps_before_non_recurring_items,
          diluted_net_eps,
          sales_estimates_consensus,
          sales_estimates_count,
          sales_estimates_grew,
          sales_estimates_declined,
          sales_consensus_change,
          sales_consensus_change_percent,
          sales_consensus_range,
          sales_dispersion_ratio,
          sales_consensus_optimist,
          sales_consensus_pessimist,
          sales_surprise_amount,
          sales_surprise_percent
        ),
        ~ zoo::na.locf(.x, na.rm = FALSE)
      )
    ) %>%
    # 6. Compute valuation ratios using current price
    dplyr::mutate(
      # Market capitalisation
      market_cap = close * shares_outstanding,

      # Price-to-Book ratio
      price_to_book = dplyr::if_else(
        abs(book_value_per_share) < 1e-10 | is.na(book_value_per_share),
        NA_real_,
        close / book_value_per_share
      ),

      # Price-to-Sales ratio (annualised for quarters)
      annualised_sales = dplyr::if_else(
        grepl("Quarter", period),
        sales * 4,
        sales
      ),
      price_to_sales = dplyr::if_else(
        abs(annualised_sales) < 1e-10 | is.na(annualised_sales),
        NA_real_,
        market_cap / annualised_sales
      ),

      # Price-to-Earnings ratio (trailing, annualised)
      annualised_net_income = dplyr::if_else(
        grepl("Quarter", period),
        net_income * 4,
        net_income
      ),
      price_to_earnings = dplyr::if_else(
        abs(annualised_net_income) < 1e-10 | is.na(annualised_net_income),
        NA_real_,
        market_cap / annualised_net_income
      ),

      # Earnings yield (inverse P/E)
      earnings_yield = dplyr::if_else(
        is.na(price_to_earnings) | abs(price_to_earnings) < 1e-10,
        NA_real_,
        1 / price_to_earnings * 100
      ),

      # Gross margin
      gross_margin = dplyr::if_else(
        abs(sales) < 1e-10 | is.na(sales),
        NA_real_,
        gross_profit / sales * 100
      ),

      # Net margin
      net_margin = dplyr::if_else(
        abs(sales) < 1e-10 | is.na(sales),
        NA_real_,
        net_income / sales * 100
      ),

      # Return on equity (annualised)
      return_on_equity = dplyr::if_else(
        abs(total_equity) < 1e-10 | is.na(total_equity),
        NA_real_,
        annualised_net_income / total_equity * 100
      ),

      # Enterprise value proxy (market cap + total liabilities - equity ≈ market cap + debt)
      enterprise_value_proxy = market_cap +
        (total_liabilities_and_equity - total_equity),

      # EV/Sales
      ev_to_sales = dplyr::if_else(
        abs(annualised_sales) < 1e-10 | is.na(annualised_sales),
        NA_real_,
        enterprise_value_proxy / annualised_sales
      )
    ) %>%
    dplyr::ungroup()

  # 7. Select final columns
  result <- result %>%
    dplyr::select(
      symbol,
      open_time,
      # Fundamental release flag
      is_fundamental_release,
      # Balance sheet
      common_stock,
      retained_earnings,
      total_equity,
      shares_outstanding,
      book_value_per_share,
      total_liabilities_and_equity,
      # Income statement
      sales,
      gross_profit,
      net_income,
      average_shares,
      diluted_net_eps,
      # Valuation ratios
      market_cap,
      price_to_book,
      price_to_sales,
      price_to_earnings,
      earnings_yield,
      gross_margin,
      net_margin,
      return_on_equity,
      enterprise_value_proxy,
      ev_to_sales,
      # Sales estimates & surprise
      sales_estimates_consensus,
      sales_estimates_count,
      sales_estimates_grew,
      sales_estimates_declined,
      sales_consensus_change,
      sales_consensus_change_percent,
      sales_surprise_amount,
      sales_surprise_percent,
      sales_consensus_optimist,
      sales_consensus_pessimist,
      # Keep remaining price columns
      dplyr::everything()
    )

  return(result)
}

#' Get leverage and solvency data
#' @description Retrieves balance sheet liabilities and assets data, merges with daily price data,
#' forward-fills, and computes leverage/solvency metrics: debt-to-equity, current ratio,
#' net debt, proper enterprise value, and EV/EBITDA.
#'
#' @details
#' **Use cases:**
#' - Financial distress early warning: current ratio < 1, rising debt-to-equity
#' - Leverage-adjusted valuation: EV/EBITDA is the most used institutional metric for buyouts/comps
#' - Risk management: screen out over-leveraged companies before earnings events
#' - Credit cycle analysis: track aggregate leverage trends across sectors
#'
#' **Complementary functions:**
#' - \code{\link{get_valuation_data}}: P/B, P/S, P/E, margins
#' - \code{\link{get_cash_flow_data}}: FCF, P/FCF, operating CF margin
#' - \code{\link{get_full_fundamentals}}: convenience wrapper combining all three
#'
#' @param db_con A DBI connection object.
#' @param symbols_price_data A data frame containing at least 'symbol', 'open_time', and 'close' columns.
#' @param start_date A character string representing the start date for filtering (YYYY-MM-DD).
#' @param end_date A character string representing the end date for filtering (YYYY-MM-DD).
#' @param period A character string indicating the period type ("Quarter" or "Year").
#' @return A data frame with daily price rows enriched with forward-filled leverage
#' and solvency columns.
#' @export
get_leverage_data <- function(
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

  # 2. Retrieve liabilities data
  liabilities_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      total_current_liabilities,
      long_term_debt,
      current_portion_long_term_debt,
      total_liabilities,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM balance_sheet_liabilities
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    liabilities_query <- paste0(
      liabilities_query,
      " AND date >= '",
      start_date,
      "'"
    )
  }
  if (!is.null(end_date)) {
    liabilities_query <- paste0(
      liabilities_query,
      " AND date <= '",
      end_date,
      "'"
    )
  }
  liabilities_query <- paste0(
    liabilities_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  liabilities <- tryCatch(
    DBI::dbGetQuery(db_con, liabilities_query) %>% dplyr::distinct(),
    error = function(e) tibble::tibble()
  )

  # 3. Retrieve assets data
  assets_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      cash_and_equivalents,
      total_current_assets,
      total_assets,
      inventories,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM balance_sheet_assets
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    assets_query <- paste0(assets_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    assets_query <- paste0(assets_query, " AND date <= '", end_date, "'")
  }
  assets_query <- paste0(
    assets_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  assets <- tryCatch(
    DBI::dbGetQuery(db_con, assets_query) %>% dplyr::distinct(),
    error = function(e) tibble::tibble()
  )

  # 4. Retrieve income statement for EBITDA calculation
  income_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      net_income AS income_net_income,
      interest_expense,
      income_taxes,
      depreciation_and_amortization,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM income_statement
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    income_query <- paste0(income_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    income_query <- paste0(income_query, " AND date <= '", end_date, "'")
  }
  income_query <- paste0(
    income_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  income_for_ebitda <- tryCatch(
    DBI::dbGetQuery(db_con, income_query) %>% dplyr::distinct(),
    error = function(e) tibble::tibble()
  )

  # 5. Retrieve equity for D/E calculation
  equity_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      total_equity,
      shares_outstanding,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM balance_sheet_equity
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    equity_query <- paste0(equity_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    equity_query <- paste0(equity_query, " AND date <= '", end_date, "'")
  }
  equity_query <- paste0(
    equity_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  equity <- tryCatch(
    DBI::dbGetQuery(db_con, equity_query) %>% dplyr::distinct(),
    error = function(e) tibble::tibble()
  )

  # 6. Get earnings calendar for open_time alignment
  earnings_calendar <- tryCatch(
    get_earnings_calendar(db_con),

    error = function(e) {
      tibble::tibble(
        symbol = character(),
        date = as.Date(character()),
        period_label = character(),
        period_end_date = as.Date(character())
      )
    }
  )

  # 7. Merge all components
  leverage_snapshot <- liabilities %>%
    dplyr::filter(grepl(period_filter, period)) %>%
    dplyr::left_join(
      assets,
      by = c("symbol", "period_end_date", "period", "period_label")
    ) %>%
    dplyr::left_join(
      equity,
      by = c("symbol", "period_end_date", "period", "period_label")
    ) %>%
    dplyr::left_join(
      income_for_ebitda,
      by = c("symbol", "period_end_date", "period", "period_label")
    ) %>%
    dplyr::left_join(
      earnings_calendar,
      by = c("symbol", "period_label", "period_end_date")
    ) %>%
    dplyr::mutate(
      date = dplyr::if_else(
        is.na(date),
        period_end_date + lubridate::days(30),
        date
      ),
      open_time = dplyr::case_when(
        !is.na(before_open) & before_open ~ date,
        !is.na(after_close) & after_close ~ date + lubridate::days(1),
        TRUE ~ date + lubridate::days(1)
      ),
      is_leverage_release = TRUE
    ) %>%
    dplyr::select(
      symbol,
      open_time,
      period_label,
      period_end_date,
      period,
      is_leverage_release,
      total_current_liabilities,
      long_term_debt,
      current_portion_long_term_debt,
      total_liabilities,
      cash_and_equivalents,
      total_current_assets,
      total_assets,
      inventories,
      total_equity,
      shares_outstanding,
      income_net_income,
      interest_expense,
      income_taxes,
      depreciation_and_amortization
    )

  # 8. Join to price data and forward-fill
  result <- symbols_price_data %>%
    dplyr::left_join(leverage_snapshot, by = c("symbol", "open_time")) %>%
    dplyr::group_by(symbol) %>%
    dplyr::arrange(symbol, open_time) %>%
    dplyr::mutate(
      is_leverage_release = dplyr::if_else(
        is.na(is_leverage_release),
        FALSE,
        TRUE
      ),
      dplyr::across(
        c(
          period_label,
          period_end_date,
          period,
          total_current_liabilities,
          long_term_debt,
          current_portion_long_term_debt,
          total_liabilities,
          cash_and_equivalents,
          total_current_assets,
          total_assets,
          inventories,
          total_equity,
          shares_outstanding,
          income_net_income,
          interest_expense,
          income_taxes,
          depreciation_and_amortization
        ),
        ~ zoo::na.locf(.x, na.rm = FALSE)
      )
    ) %>%
    # 9. Compute leverage and solvency ratios
    dplyr::mutate(
      # Net debt = total debt - cash
      total_debt = dplyr::coalesce(long_term_debt, 0) +
        dplyr::coalesce(current_portion_long_term_debt, 0),
      net_debt = total_debt - dplyr::coalesce(cash_and_equivalents, 0),

      # Debt-to-Equity
      debt_to_equity = dplyr::if_else(
        abs(total_equity) < 1e-10 | is.na(total_equity),
        NA_real_,
        total_debt / total_equity
      ),

      # Current Ratio
      current_ratio = dplyr::if_else(
        abs(total_current_liabilities) < 1e-10 |
          is.na(total_current_liabilities),
        NA_real_,
        total_current_assets / total_current_liabilities
      ),

      # Quick Ratio (current assets - inventories) / current liabilities
      quick_ratio = dplyr::if_else(
        abs(total_current_liabilities) < 1e-10 |
          is.na(total_current_liabilities),
        NA_real_,
        (total_current_assets - dplyr::coalesce(inventories, 0)) /
          total_current_liabilities
      ),

      # Proper Enterprise Value = market_cap + net_debt
      market_cap = close * shares_outstanding,
      enterprise_value = dplyr::if_else(
        is.na(market_cap) | is.na(net_debt),
        NA_real_,
        market_cap + net_debt
      ),

      # EBITDA (annualised for quarters)
      ebitda = dplyr::coalesce(income_net_income, 0) +
        dplyr::coalesce(interest_expense, 0) +
        dplyr::coalesce(income_taxes, 0) +
        dplyr::coalesce(depreciation_and_amortization, 0),
      annualised_ebitda = dplyr::if_else(
        grepl("Quarter", period),
        ebitda * 4,
        ebitda
      ),

      # EV/EBITDA
      ev_to_ebitda = dplyr::if_else(
        abs(annualised_ebitda) < 1e-10 | is.na(annualised_ebitda),
        NA_real_,
        enterprise_value / annualised_ebitda
      ),

      # Net Debt / EBITDA (leverage coverage)
      net_debt_to_ebitda = dplyr::if_else(
        abs(annualised_ebitda) < 1e-10 | is.na(annualised_ebitda),
        NA_real_,
        net_debt / annualised_ebitda
      )
    ) %>%
    dplyr::ungroup()

  # 10. Select final columns
  result <- result %>%
    dplyr::select(
      symbol,
      open_time,
      is_leverage_release,
      # Debt & liquidity
      total_debt,
      net_debt,
      cash_and_equivalents,
      total_current_liabilities,
      total_current_assets,
      total_liabilities,
      total_assets,
      # Ratios
      debt_to_equity,
      current_ratio,
      quick_ratio,
      # Enterprise value
      enterprise_value,
      ev_to_ebitda,
      net_debt_to_ebitda,
      ebitda,
      annualised_ebitda,
      # Keep remaining price columns
      dplyr::everything()
    ) %>%
    dplyr::select(
      -c(
        period_label,
        period_end_date,
        period,
        is_leverage_release,
        long_term_debt,
        current_portion_long_term_debt,
        inventories,
        total_equity,
        shares_outstanding,
        market_cap,
        income_net_income,
        interest_expense,
        income_taxes,
        depreciation_and_amortization
      )
    )

  return(result)
}

#' Get cash flow data
#' @description Retrieves cash flow statement data, merges with daily price data,
#' forward-fills, and computes free cash flow metrics: FCF, P/FCF, FCF yield,
#' and operating cash flow margin.
#'
#' @details
#' **Use cases:**
#' - Quality screening: companies with positive FCF are less likely to manipulate earnings
#' - Buyback/dividend sustainability: FCF yield > dividend yield signals safe payout
#' - Growth vs. cash generation: high capex ratio flags aggressive reinvestment
#' - Distress detection: negative operating CF despite positive net income = red flag
#'
#' **Complementary functions:**
#' - \code{\link{get_valuation_data}}: P/B, P/S, P/E, margins
#' - \code{\link{get_leverage_data}}: debt/equity, current ratio, EV/EBITDA
#' - \code{\link{get_full_fundamentals}}: convenience wrapper combining all three
#'
#' @param db_con A DBI connection object.
#' @param symbols_price_data A data frame containing at least 'symbol', 'open_time', and 'close' columns.
#' @param start_date A character string representing the start date for filtering (YYYY-MM-DD).
#' @param end_date A character string representing the end date for filtering (YYYY-MM-DD).
#' @param period A character string indicating the period type ("Quarter" or "Year").
#' @return A data frame with daily price rows enriched with forward-filled cash flow columns.
#' @export
get_cash_flow_data <- function(
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

  # 2. Retrieve cash flow statement
  cf_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      net_income AS cf_net_income,
      depreciation_amortization_and_depletion,
      net_cash_from_operating_activities,
      property_and_equipment AS capital_expenditures,
      net_cash_from_investing_activities,
      payment_of_dividends_and_other_distributions,
      net_cash_from_financing_activities,
      net_change_in_cash_and_equivalents,
      cash_at_end_of_period,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM cash_flow_statement
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    cf_query <- paste0(cf_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    cf_query <- paste0(cf_query, " AND date <= '", end_date, "'")
  }
  cf_query <- paste0(
    cf_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  cash_flow <- tryCatch(
    DBI::dbGetQuery(db_con, cf_query) %>%
      dplyr::distinct() %>%
      dplyr::filter(grepl(period_filter, period)),
    error = function(e) tibble::tibble()
  )

  # 3. Retrieve sales for margin calculation
  sales_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      sales,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM income_statement
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    sales_query <- paste0(sales_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sales_query <- paste0(sales_query, " AND date <= '", end_date, "'")
  }
  sales_query <- paste0(
    sales_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  sales_data <- tryCatch(
    DBI::dbGetQuery(db_con, sales_query) %>%
      dplyr::distinct() %>%
      dplyr::filter(grepl(period_filter, period)),
    error = function(e) tibble::tibble()
  )

  # 4. Retrieve shares outstanding for per-share calculations
  shares_query <- "
    SELECT
      act_symbol AS symbol,
      date AS period_end_date,
      period,
      shares_outstanding,
      CASE
        WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY))
        WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM date - INTERVAL 63 DAY) AS VARCHAR)
      END AS period_label
    FROM balance_sheet_equity
    WHERE 1 = 1"
  if (!is.null(start_date)) {
    shares_query <- paste0(shares_query, " AND date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    shares_query <- paste0(shares_query, " AND date <= '", end_date, "'")
  }
  shares_query <- paste0(
    shares_query,
    " AND act_symbol IN ('",
    paste(symbol, collapse = "', '"),
    "')",
    " ORDER BY act_symbol, date;"
  )

  shares_data <- tryCatch(
    DBI::dbGetQuery(db_con, shares_query) %>%
      dplyr::distinct() %>%
      dplyr::filter(grepl(period_filter, period)),
    error = function(e) tibble::tibble()
  )

  # 5. Get earnings calendar for open_time alignment
  earnings_calendar <- tryCatch(
    get_earnings_calendar(db_con),
    error = function(e) {
      tibble::tibble(
        symbol = character(),
        date = as.Date(character()),
        period_label = character(),
        period_end_date = as.Date(character())
      )
    }
  )

  # 6. Merge components
  cf_snapshot <- cash_flow %>%
    dplyr::left_join(
      sales_data %>%
        dplyr::select(symbol, period_end_date, period_label, sales),
      by = c("symbol", "period_end_date", "period_label")
    ) %>%
    dplyr::left_join(
      shares_data %>%
        dplyr::select(
          symbol,
          period_end_date,
          period_label,
          shares_outstanding
        ),
      by = c("symbol", "period_end_date", "period_label")
    ) %>%
    dplyr::left_join(
      earnings_calendar,
      by = c("symbol", "period_label", "period_end_date")
    ) %>%
    dplyr::mutate(
      date = dplyr::if_else(
        is.na(date),
        period_end_date + lubridate::days(30),
        date
      ),
      open_time = dplyr::case_when(
        !is.na(before_open) & before_open ~ date,
        !is.na(after_close) & after_close ~ date + lubridate::days(1),
        TRUE ~ date + lubridate::days(1)
      ),
      is_cash_flow_release = TRUE
    ) %>%
    dplyr::select(
      symbol,
      open_time,
      period_label,
      period_end_date,
      period,
      is_cash_flow_release,
      cf_net_income,
      depreciation_amortization_and_depletion,
      net_cash_from_operating_activities,
      capital_expenditures,
      net_cash_from_investing_activities,
      payment_of_dividends_and_other_distributions,
      net_cash_from_financing_activities,
      net_change_in_cash_and_equivalents,
      cash_at_end_of_period,
      sales,
      shares_outstanding
    )

  # 7. Join to price data and forward-fill
  result <- symbols_price_data %>%
    dplyr::left_join(cf_snapshot, by = c("symbol", "open_time")) %>%
    dplyr::group_by(symbol) %>%
    dplyr::arrange(symbol, open_time) %>%
    dplyr::mutate(
      is_cash_flow_release = dplyr::if_else(
        is.na(is_cash_flow_release),
        FALSE,
        TRUE
      ),
      dplyr::across(
        c(
          period_label,
          period_end_date,
          period,
          cf_net_income,
          depreciation_amortization_and_depletion,
          net_cash_from_operating_activities,
          capital_expenditures,
          net_cash_from_investing_activities,
          payment_of_dividends_and_other_distributions,
          net_cash_from_financing_activities,
          net_change_in_cash_and_equivalents,
          cash_at_end_of_period,
          sales,
          shares_outstanding
        ),
        ~ zoo::na.locf(.x, na.rm = FALSE)
      )
    ) %>%
    # 8. Compute cash flow metrics
    dplyr::mutate(
      # Free Cash Flow = Operating CF - Capex (capex is typically negative, so we add)
      free_cash_flow = net_cash_from_operating_activities +
        dplyr::coalesce(capital_expenditures, 0),

      # Annualise for quarters
      annualised_fcf = dplyr::if_else(
        grepl("Quarter", period),
        free_cash_flow * 4,
        free_cash_flow
      ),
      annualised_operating_cf = dplyr::if_else(
        grepl("Quarter", period),
        net_cash_from_operating_activities * 4,
        net_cash_from_operating_activities
      ),

      # Market cap for ratios
      market_cap = close * shares_outstanding,

      # Price-to-FCF
      price_to_fcf = dplyr::if_else(
        abs(annualised_fcf) < 1e-10 | is.na(annualised_fcf),
        NA_real_,
        market_cap / annualised_fcf
      ),

      # FCF Yield (%)
      fcf_yield = dplyr::if_else(
        is.na(market_cap) | abs(market_cap) < 1e-10,
        NA_real_,
        annualised_fcf / market_cap * 100
      ),

      # Operating CF Margin (%)
      annualised_sales = dplyr::if_else(
        grepl("Quarter", period),
        sales * 4,
        sales
      ),
      operating_cf_margin = dplyr::if_else(
        abs(annualised_sales) < 1e-10 | is.na(annualised_sales),
        NA_real_,
        annualised_operating_cf / annualised_sales * 100
      ),

      # Capex ratio (capex as % of operating CF — measures reinvestment intensity)
      capex_to_operating_cf = dplyr::if_else(
        abs(net_cash_from_operating_activities) < 1e-10 |
          is.na(net_cash_from_operating_activities),
        NA_real_,
        abs(capital_expenditures) / net_cash_from_operating_activities * 100
      ),

      # Cash flow quality: operating CF / net income (>1 = high quality)
      cf_quality = dplyr::if_else(
        abs(cf_net_income) < 1e-10 | is.na(cf_net_income),
        NA_real_,
        net_cash_from_operating_activities / cf_net_income
      ),

      # FCF per share
      fcf_per_share = dplyr::if_else(
        is.na(shares_outstanding) | shares_outstanding < 1,
        NA_real_,
        free_cash_flow / shares_outstanding
      )
    ) %>%
    dplyr::ungroup()

  # 9. Select final columns
  result <- result %>%
    dplyr::select(
      symbol,
      open_time,
      is_cash_flow_release,
      # Core cash flow
      net_cash_from_operating_activities,
      capital_expenditures,
      free_cash_flow,
      net_cash_from_financing_activities,
      payment_of_dividends_and_other_distributions,
      cash_at_end_of_period,
      # Ratios
      price_to_fcf,
      fcf_yield,
      operating_cf_margin,
      capex_to_operating_cf,
      cf_quality,
      fcf_per_share,
      # Keep remaining price columns
      dplyr::everything()
    ) %>%
    dplyr::select(
      -c(
        period_label,
        period_end_date,
        period,
        cf_net_income,
        depreciation_amortization_and_depletion,
        net_cash_from_investing_activities,
        net_change_in_cash_and_equivalents,
        sales,
        shares_outstanding,
        market_cap,
        annualised_fcf,
        annualised_operating_cf,
        annualised_sales
      )
    )

  return(result)
}

#' Get full fundamental data (valuation + leverage + cash flow)
#' @description Convenience wrapper that calls \code{get_valuation_data()},
#' \code{get_leverage_data()}, and \code{get_cash_flow_data()} and merges
#' the results into a single data frame joined on symbol and open_time.
#'
#' @details
#' **Use cases:**
#' - Comprehensive fundamental factor models (multi-factor quant strategies)
#' - Full company health screening: combines profitability, leverage, and cash generation
#' - Backtesting composite value/quality scores (e.g., Piotroski F-Score inputs)
#' - Building training datasets for ML models predicting returns from fundamental features
#'
#' @param db_con A DBI connection object.
#' @param symbols_price_data A data frame containing at least 'symbol', 'open_time', and 'close' columns.
#' @param start_date A character string representing the start date for filtering (YYYY-MM-DD).
#' @param end_date A character string representing the end date for filtering (YYYY-MM-DD).
#' @param period A character string indicating the period type ("Quarter" or "Year").
#' @return A data frame with daily price rows enriched with all fundamental columns.
#' @export
get_full_fundamentals <- function(
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

  # 2. Call each component
  valuation <- get_valuation_data(
    db_con,
    symbols_price_data,
    start_date,
    end_date,
    period
  )
  leverage <- get_leverage_data(
    db_con,
    symbols_price_data,
    start_date,
    end_date,
    period
  )
  cash_flow <- get_cash_flow_data(
    db_con,
    symbols_price_data,
    start_date,
    end_date,
    period
  )

  # 3. Identify price columns to avoid duplication
  price_cols <- names(symbols_price_data)
  join_cols <- c("symbol", "open_time")

  # Columns to keep from leverage (exclude duplicated price columns)
  leverage_new <- leverage %>%
    dplyr::select(-dplyr::any_of(setdiff(price_cols, join_cols)))

  # Columns to keep from cash_flow (exclude duplicated price columns)
  cash_flow_new <- cash_flow %>%
    dplyr::select(-dplyr::any_of(setdiff(price_cols, join_cols)))

  # 4. Merge
  result <- valuation %>%
    dplyr::left_join(leverage_new, by = join_cols) %>%
    dplyr::left_join(cash_flow_new, by = join_cols) %>%
    dplyr::relocate(
      open,
      high,
      low,
      close,
      volume,
      adjusted,
      period_label,
      .after = open_time
    )

  return(result)
}
