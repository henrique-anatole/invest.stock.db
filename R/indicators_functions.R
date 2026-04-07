#' Get Simple Moving Averages from Database
#' @param db_con A DBI connection object.
#' @param timeframe Character. "1d" or "1h".
#' @param periods Numeric vector of periods.
#' @return A data.frame with SMA columns.
#' @export
get_smas <- function(db_con, timeframe = "1d", periods = c(20, 50, 150, 200)) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(periods) || any(periods <= 0) || length(periods) == 0) {
    stop("'periods' must be a vector of positive integers.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # 2. Build SQL Columns
  # Corrected vapply usage: character(1) instead of CHARACTER(1)
  sma_cols <- vapply(
    periods,
    function(p) {
      paste0(
        "CASE WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time) >= ",
        p,
        " THEN AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ",
        " ROWS BETWEEN ",
        (p - 1),
        " PRECEDING AND CURRENT ROW) ",
        " ELSE NULL END AS SMA_",
        p
      )
    },
    character(1)
  )

  # 3. Construct and Execute Query
  # Using paste(collapse) is cleaner than a for-loop
  full_query <- paste0(
    "SELECT symbol, open_time, ",
    paste(sma_cols, collapse = ", "),
    " FROM ",
    table_to_query
  )

  result <- DBI::dbGetQuery(db_con, full_query)

  return(result)
}

# Optimized for vector input
calculate_single_ema <- function(x, period) {
  # 1. Handle NAs (Carry forward)
  x <- zoo::na.locf(x, na.rm = FALSE)

  # 2. Validation: Need enough non-NA values for the period
  if (sum(!is.na(x)) < period) {
    return(rep(as.numeric(NA), length(x)))
  }

  # 3. Calculate
  return(TTR::EMA(x, n = period))
}

#' Calculate Multiple EMAs for a Timeseries
#'
#' @param timeseries A data frame containing 'symbol', 'close', and 'open_time'.
#' @param periods A numeric vector of periods (e.g., 20, 50).
#' @return A grouped_df (or ungrouped df if fixed) with new columns 'ema_P'.
#' @export
get_emas <- function(timeseries, periods = c(20, 50, 150, 200)) {
  # Check if the required columns exist
  if (!("symbol" %in% colnames(timeseries))) {
    stop("The timeseries must have a 'symbol' column.")
  }
  if (!("close" %in% colnames(timeseries))) {
    stop("The timeseries must have a 'close' column.")
  }
  if (!("open_time" %in% colnames(timeseries))) {
    stop("The timeseries must have an 'open_time' column.")
  }

  # Ensure 'periods' has at least one value
  if (!is.numeric(periods) || any(periods <= 0) || length(periods) == 0) {
    stop("'periods' must be a vector of positive integers.")
  }

  # Sort data by 'open_time' before calculation
  timeseries <- timeseries %>%
    arrange(symbol, open_time) %>%
    group_by(symbol)

  # Loop over all provided periods and calculate EMA for each
  for (period in periods) {
    # Create a new column name for the EMA
    ema_col_name <- paste0("EMA_", period)

    # Calculate EMA for the current period and add it to the dataframe
    timeseries <- timeseries %>%
      mutate(!!ema_col_name := calculate_single_ema(close, period))
  }
  # Un-group the data frame after calculations
  timeseries <- timeseries %>%
    ungroup()
  # Return the timeseries with the new columns
  return(timeseries)
}

#' Get Bollinger Bands and Distance Metrics from Database
#'
#' @description
#' Generates a SQL query to calculate Bollinger Bands (SMA +/- 2SD) and the
#' normalized distance of Close, High, and Low prices from those bands.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character. Supported values: "1d", "1h".
#' @param period Integer. The window size for the calculation (default 20).
#'
#' @return A data frame containing:
#' \itemize{
#'   \item \code{symbol}, \code{open_time}
#'   \item \code{SMA_P}, \code{Upper_Band_P}, \code{Lower_Band_P}
#'   \item \code{Distance_Upper_Band_P}: (Close - Upper) / 2SD
#'   \item \code{Distance_Lower_Band_P}: (Close - Lower) / 2SD
#'   \item \code{Distance_High_Upper_Band_P}: (High - Upper) / 2SD
#'   \item \code{Distance_Low_Lower_Band_P}: (Low - Lower) / 2SD
#' }
#' @export
get_bollinger_bands <- function(db_con, timeframe = "1d", period = 20) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # Define the window once to avoid repetition/typos
  win <- paste0(
    "OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW)"
  )

  # Building the query as a single coherent string
  select_query <- paste0(
    "
    SELECT 
      symbol, 
      open_time,
      AVG(close) ",
    win,
    " AS SMA_",
    period,
    ",
      STDDEV_POP(close) ",
    win,
    " AS STDDEV_",
    period,
    ",
      -- Bands
      (AVG(close) ",
    win,
    " + (2 * STDDEV_POP(close) ",
    win,
    ")) AS Upper_Band_",
    period,
    ",
      (AVG(close) ",
    win,
    " - (2 * STDDEV_POP(close) ",
    win,
    ")) AS Lower_Band_",
    period,
    ",
      -- Distances
      (close - (AVG(close) ",
    win,
    " + (2 * STDDEV_POP(close) ",
    win,
    "))) / NULLIF(2 * STDDEV_POP(close) ",
    win,
    ", 0) AS Distance_Upper_Band_",
    period,
    ",
      (close - (AVG(close) ",
    win,
    " - (2 * STDDEV_POP(close) ",
    win,
    "))) / NULLIF(2 * STDDEV_POP(close) ",
    win,
    ", 0) AS Distance_Lower_Band_",
    period,
    ",
      (high - (AVG(close) ",
    win,
    " + (2 * STDDEV_POP(close) ",
    win,
    "))) / NULLIF(2 * STDDEV_POP(close) ",
    win,
    ", 0) AS Distance_High_Upper_Band_",
    period,
    ",
      (low - (AVG(close) ",
    win,
    " - (2 * STDDEV_POP(close) ",
    win,
    "))) / NULLIF(2 * STDDEV_POP(close) ",
    win,
    ", 0) AS Distance_Low_Lower_Band_",
    period,
    "
    FROM ",
    table_to_query,
    "
    ORDER BY symbol, open_time
  "
  )

  result <- DBI::dbGetQuery(db_con, select_query)

  # Exclude the first rows where there is insufficient data to calculate Bollinger Bands
  result <- result %>%
    group_by(symbol) %>%
    filter(row_number() > period) %>%
    ungroup()

  return(result)
}

#' Calculate MACD, Signal Line, and Histogram
#'
#' @param timeseries Dataframe with symbol, open_time, and close.
#' @param fast_period Integer. Fast EMA window (default 12).
#' @param slow_period Integer. Slow EMA window (default 26).
#' @param signal_period Integer. Signal line EMA window (default 9).
#' @param maType Character. Moving average type (default "EMA").
#' @param percent Logical. Use percentage difference instead of absolute (default TRUE).
#' @return An ungrouped dataframe with 4 new columns: macd, signal_line, macd_histogram, macd_direction.
#'
#' @export
get_macd <- function(
  timeseries,
  fast_period = 12,
  slow_period = 26,
  signal_period = 9,
  maType = "EMA",
  percent = TRUE
) {
  # 1. Validations
  required_cols <- c("symbol", "open_time", "close")
  if (!all(required_cols %in% colnames(timeseries))) {
    stop(
      "Missing required columns: ",
      paste(setdiff(required_cols, colnames(timeseries)), collapse = ", ")
    )
  }

  # 2. Processing
  timeseries <- timeseries %>%
    arrange(symbol, open_time) %>%
    group_by(symbol) %>%
    # Ensure sufficient data exists to avoid TTR errors
    filter(n() >= (slow_period + signal_period)) %>%
    mutate(
      # Fill NAs in price only
      clean_close = zoo::na.locf(close, na.rm = FALSE),
      # Calculate MACD once as a matrix
      macd_mat = TTR::MACD(
        clean_close,
        nFast = fast_period,
        nSlow = slow_period,
        nSig = signal_period,
        maType = maType,
        percent = percent
      ),
      macd = macd_mat[, 1],
      signal_line = macd_mat[, 2],
      macd_histogram = macd - signal_line,
      macd_direction = if_else(macd_histogram > 0, 1, 0)
    ) %>%
    select(-macd_mat, -clean_close) %>% # Clean up temp columns
    ungroup() %>%
    filter(!is.na(signal_line))

  return(timeseries)
}

#' Get Volatility Indicators from Database
#'
#' @description
#' Calculates Intraday Volatility, True Range, Price Gaps, Standard Deviation,
#' and ATR (Average True Range) using SQL window functions.
#'
#' @param db_con A DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param periods Numeric vector of periods for Volatility and ATR.
#' @return A data frame with volatility metrics for each specified period.
#' @export
get_volatilities <- function(
  db_con,
  timeframe = "1d",
  periods = c(10, 20, 50)
) {
  # ... [Validations] ...
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(periods) || any(periods <= 0) || length(periods) == 0) {
    stop("'periods' must be a vector of positive integers.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # 2. Row-level metrics in CTE
  cte_query <- paste0(
    "
    WITH price_data AS (
        SELECT symbol, open_time, open, high, low, close,
            (high - low) AS intraday_volatility,
            GREATEST(
                high - low,
                ABS(high - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)),
                ABS(low - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time))
            ) AS true_range,
            CASE WHEN open < LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 THEN (open - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / NULLIF(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time), 0)
                 ELSE NULL END AS gap_down,
            CASE WHEN open > LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 THEN (open - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / NULLIF(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time), 0)
                 ELSE NULL END AS gap_up
        FROM ",
    table_to_query,
    "
    )"
  )

  # 3. Dynamic Window Metrics
  window_cols <- vapply(
    periods,
    function(p) {
      paste0(
        ", STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
        p - 1,
        " PRECEDING AND CURRENT ROW) AS volatility_",
        p,
        ", AVG(true_range) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
        p - 1,
        " PRECEDING AND CURRENT ROW) AS atr_",
        p
      )
    },
    character(1)
  )

  full_query <- paste0(
    cte_query,
    " SELECT symbol, open_time, intraday_volatility, true_range, gap_down, gap_up",
    paste(window_cols, collapse = ""),
    " FROM price_data ORDER BY symbol, open_time"
  )

  result <- DBI::dbGetQuery(db_con, full_query) %>%
    group_by(symbol) %>%
    filter(row_number() >= min(periods)) %>% # Exclude warm-up rows
    ungroup()

  return(result)
}

#' Check for Recent Significant Price Gaps
#'
#' @description
#' Scans a timeseries for gaps exceeding a percentage threshold within a
#' rolling lookback window.
#'
#' @param timeseries Dataframe containing 'gap_up' and 'gap_down'.
#' @param gap_size Numeric. The threshold (e.g., 0.03 for 3%).
#' @param periods Integer. The lookback window size (default 5).
#' @return The original dataframe with 'significant_gap' and 'recent_periods_gap' columns.
#' @export
check_recent_gaps <- function(timeseries, gap_size = 0.03, periods = 5) {
  if (!all(c("gap_up", "gap_down") %in% colnames(timeseries))) {
    stop("The timeseries must have 'gap_up' and 'gap_down' columns.")
  }

  timeseries <- timeseries %>%
    group_by(symbol) %>%
    mutate(
      # Magnitude of the gap regardless of direction
      significant_gap = coalesce(gap_up, abs(gap_down), 0) > gap_size
    )

  # Support multiple lookback periods if provided
  for (p in periods) {
    col_name <- paste0("recent_gap_", p)
    timeseries <- timeseries %>%
      mutate(
        !!col_name := slider::slide_lgl(
          significant_gap,
          .f = ~ any(.x, na.rm = TRUE),
          .before = p - 1,
          .complete = TRUE
        )
      )
  }

  return(ungroup(timeseries))
}


#' Get Donchian Channels (Rolling Min/Max) from Database
#' @description Calculates the rolling High and Low bounds and the Mid-point.
#' @param db_con A DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. Lookback window (default 52).
#' @return A data frame with upper_donchian_P, lower_donchian_P, and mid_donchian_P.
#' @export
get_donchian_channels <- function(db_con, timeframe = "1d", period = 52) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # Construct the SQL Query
  # Note: Using MAX(high) and MIN(low) as suggested for standard channels
  select_query <- paste0(
    "
    SELECT 
        symbol, 
        open_time, 
        MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS upper_donchian_",
    period,
    ",
        MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS lower_donchian_",
    period,
    "
    FROM ",
    table_to_query
  )

  # Execute the query and return the results as a data frame
  result <- DBI::dbGetQuery(db_con, select_query)

  # Calculate the Mid-Channel point and filter warm-up rows
  result <- result %>%
    dplyr::group_by(symbol) %>%
    dplyr::mutate(
      !!paste0("mid_donchian_", period) := (get(paste0(
        "upper_donchian_",
        period
      )) +
        get(paste0("lower_donchian_", period))) /
        2
    ) %>%
    # Exclude rows where there is insufficient data for a full window
    dplyr::filter(dplyr::row_number() >= period) %>%
    dplyr::ungroup()

  return(result)
}

#' Get Daily or Weekly Pivot Points
#' @description
#' Calculates Floor (Regular) or Fibonacci Pivot Points. When 'weekly' is TRUE,
#' it aggregates daily data into weeks, calculates the levels, and joins them
#' back to every daily row for that week.
#' @param db_con A DBI connection object (e.g., DuckDB, Postgres).
#' @param weekly Logical. If TRUE, calculates pivots based on previous week's data.
#' @param fib Logical. If TRUE, uses Fibonacci ratios. If FALSE, uses Floor formulas.
#' @return A data frame with daily price data and corresponding pivot levels.
#' @export
get_pivots <- function(db_con, weekly = TRUE, fib = FALSE) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  # 2. Define the Mathematical Formulas
  # prev_h, prev_l, and prev_c are aliases defined in the CTEs below
  if (fib) {
    # Fibonacci Levels: Pivot +/- Range * Ratios
    math_levels <- "
      pivt + (prev_h - prev_l) * 0.382 AS r1, 
      pivt + (prev_h - prev_l) * 0.618 AS r2, 
      pivt + (prev_h - prev_l) * 1.000 AS r3,
      pivt - (prev_h - prev_l) * 0.382 AS s1, 
      pivt - (prev_h - prev_l) * 0.618 AS s2, 
      pivt - (prev_h - prev_l) * 1.000 AS s3"
  } else {
    # Standard Floor Levels
    math_levels <- "
      (2 * pivt) - prev_l AS r1, 
      pivt + (prev_h - prev_l) AS r2, 
      prev_h + 2 * (pivt - prev_l) AS r3,
      (2 * pivt) - prev_h AS s1, 
      pivt - (prev_h - prev_l) AS s2, 
      prev_l - 2 * (prev_h - pivt) AS s3"
  }

  # 3. Select the Query Path
  if (weekly) {
    # WEEKLY LOGIC: Aggregate -> Calculate -> Join back to Daily
    query <- paste0(
      "
      WITH weekly_agg AS (
        SELECT 
          symbol, 
          DATE_TRUNC('week', open_time) AS week_start,
          MAX(high) AS w_high, 
          MIN(low) AS w_low, 
          LAST(close ORDER BY open_time) AS w_close
        FROM daily_prices
        GROUP BY 1, 2
      ),
      weekly_pivots AS (
        SELECT 
          symbol, 
          week_start,
          LAG(w_high) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_h,
          LAG(w_low) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_l,
          LAG(w_close) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_c
        FROM weekly_agg
      )
      SELECT 
        d.symbol, d.open_time, d.high, d.low, d.close,
        (prev_h + prev_l + prev_c) / 3 AS pivt,
        prev_h AS swing_high,
        ",
      math_levels,
      "
      FROM daily_prices d
      LEFT JOIN weekly_pivots w 
        ON d.symbol = w.symbol 
        AND DATE_TRUNC('week', d.open_time) = w.week_start
      WHERE pivt IS NOT NULL
      ORDER BY d.symbol, d.open_time
    "
    )
  } else {
    # DAILY LOGIC: Calculate using simple LAGs
    query <- paste0(
      "
      WITH daily_pivots AS (
        SELECT 
          symbol, open_time, high, low, close,
          LAG(high) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_h,
          LAG(low) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_l,
          LAG(close) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_c
        FROM daily_prices
      )
      SELECT 
        symbol, open_time, high, low, close,
        (prev_h + prev_l + prev_c) / 3 AS pivt,
        prev_h AS swing_high,
        ",
      math_levels,
      "
      FROM daily_pivots
      WHERE pivt IS NOT NULL
      ORDER BY symbol, open_time
    "
    )
  }
  # 4. Execute the query and return the results as a data frame
  result <- DBI::dbGetQuery(db_con, query)

  return(result)
}

#' Get RSI and Stochastic Oscillator
#'
#' @description
#' Calculates the Relative Strength Index (RSI) and the Stochastic Oscillator (%K)
#' using SQL window functions at the database level.
#' The code calculates a "Simple" RSI. If you wanted the official Wilder’s RSI, you would need a recursive EMA, which is significantly harder to write in standard SQL (often requiring a recursive CTE). For most strategies, the SMA version is a perfectly valid proxy.
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. The lookback period for both indicators (default 14).
#'
#' @return A data frame containing symbol, open_time, rsi, and stochastic_k.
#' @export
get_oscillators <- function(db_con, timeframe = "1d", period = 14) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # Consolidated Query: One path for RSI and Stochastics
  query <- paste0(
    "
    WITH raw_stats AS (
        SELECT
            symbol,
            open_time,
            close,
            -- For Stochastics
            MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS hi,
            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS lo,
            -- For RSI
            close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS diff
        FROM ",
    table_to_query,
    "
    ),
    rsi_calc AS (
        SELECT
            *,
            CASE WHEN diff > 0 THEN diff ELSE 0 END AS gain,
            CASE WHEN diff < 0 THEN ABS(diff) ELSE 0 END AS loss
        FROM raw_stats
    ),
    final_indicators AS (
        SELECT
            symbol,
            open_time,
            -- RSI Calculation
            100 - (100 / (1 + (
                AVG(gain) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) / 
                NULLIF(AVG(loss) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW), 0)
            ))) AS rsi,
            -- Stochastic %K Calculation
            (close - lo) / NULLIF(hi - lo, 0) * 100 AS stochastic_k
        FROM rsi_calc
    )
    SELECT * FROM final_indicators 
    ORDER BY symbol, open_time
  "
  )
  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(row_number() >= period) %>% # Exclude warm-up rows
    ungroup()

  return(result)
}


#' Get Trend Following Indicators
#'
#' @description
#' Calculates Momentum, Rate of Change (ROC), and Kaufman's Efficiency Ratio (KER)
#' using SQL window functions. Useful for identifying trend strength and noise.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. The lookback window for trend calculations (default 14).
#'
#' @return A data frame containing symbol, open_time, momentum, roc, and ker.
#' @export
get_trend_followers <- function(db_con, timeframe = "1d", period = 14) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # SQL Query
  # We use a single CTE to get the lagged prices and daily absolute changes
  query <- paste0(
    "
    WITH base_data AS (
        SELECT
            symbol,
            open_time,
            close,
            LAG(close, ",
    period,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS prev_close,
            ABS(close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) AS daily_abs_diff
        FROM ",
    table_to_query,
    "
    ),
    metrics AS (
        SELECT
            symbol,
            open_time,
            (close - prev_close) AS momentum,
            ((close - prev_close) / NULLIF(prev_close, 0)) * 100 AS roc,
            ABS(close - prev_close) AS net_change,
            SUM(daily_abs_diff) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS total_path
        FROM base_data
    )
    SELECT
        symbol,
        open_time,
        momentum,
        roc,
        net_change / NULLIF(total_path, 0) AS ker
    FROM metrics
    ORDER BY symbol, open_time
  "
  )

  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(row_number() >= period) %>% # Exclude warm-up rows
    ungroup()

  return(result)
}

#' Get Volume Oscillators (RSI Volume & Force Index)
#'
#' @description
#' Calculates volume-weighted indicators to confirm trend strength.
#' RSI Volume identifies volume momentum, while the Force Index combines
#' price change and volume to measure market power.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param rsi_period Integer. Lookback for Volume RSI (default 14).
#' @param force_index_smoothing Integer. SMA window for Force Index (default 13).
#'
#' @return A data frame containing symbol, open_time, rsi_volume, and force_index.
#' @export
get_vol_oscillators <- function(
  db_con,
  timeframe = "1d",
  rsi_period = 14,
  force_index_smoothing = 13
) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  if (length(rsi_period) != 1 || rsi_period <= 0) {
    stop("rsi_period must be a positive integer.")
  }
  if (length(force_index_smoothing) != 1 || force_index_smoothing <= 0) {
    stop("force_index_smoothing must be a positive integer.")
  }

  # Consolidated SQL Query
  query <- paste0(
    "
    WITH base_stats AS (
        SELECT
            symbol,
            open_time,
            close,
            volume,
            -- Price change for Force Index
            close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS price_diff,
            -- Volume change for Volume RSI
            volume - LAG(volume, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS vol_diff
        FROM ",
    table_to_query,
    "
    ),
    gain_loss_calc AS (
        SELECT
            *,
            CASE WHEN vol_diff > 0 THEN vol_diff ELSE 0 END AS v_gain,
            CASE WHEN vol_diff < 0 THEN ABS(vol_diff) ELSE 0 END AS v_loss,
            (price_diff * volume) AS raw_force
        FROM base_stats
    )
    SELECT
        symbol,
        open_time,
        -- RSI Volume Calculation
        100 - (100 / (1 + (
            AVG(v_gain) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    rsi_period - 1,
    " PRECEDING AND CURRENT ROW) / 
            NULLIF(AVG(v_loss) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    rsi_period - 1,
    " PRECEDING AND CURRENT ROW), 0)
        ))) AS rsi_volume,
        -- Force Index (Smoothed)
        AVG(raw_force) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    force_index_smoothing - 1,
    " PRECEDING AND CURRENT ROW) AS force_index
    FROM gain_loss_calc
    ORDER BY symbol, open_time
  "
  )

  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(row_number() >= min(rsi_period, force_index_smoothing)) %>% # Exclude warm-up rows
    ungroup()
}

#' Get Volume Averages and Anchored Indicators
#' @description
#' Calculates Anchored VWAP and OBV (Weekly reset for 1d, Daily reset for 1h)
#' along with historical Volume SMAs and Relative Volume (RVOL).
#' @param db_con A DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param periods Numeric vector for Volume SMAs (e.g., c(20, 50)).
#'
#' @return A data frame with anchored_vwap, anchored_obv, vol_sma_X, and vol_rel_X for each period.
#' @export
get_vol_averages <- function(
  db_con,
  timeframe = "1d",
  periods = c(20, 50, 150, 200)
) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(periods) || any(periods <= 0) || length(periods) == 0) {
    stop("'periods' must be a vector of positive integers.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # 2. Define Anchor Logic
  # 1h data resets every Day; 1d data resets every Week
  anchor_sql <- if (timeframe == "1d") {
    "DATE_TRUNC('week', open_time)"
  } else {
    "CAST(open_time AS DATE)"
  }

  # 3. Construct SQL
  # Part A: Base CTE with price-volume products and directional volume
  base_query <- paste0(
    "
    WITH base_stats AS (
        SELECT
            symbol,
            open_time,
            close,
            volume,
            ",
    anchor_sql,
    " AS anchor_group,
            CASE 
                WHEN close > LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) THEN volume
                WHEN close < LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) THEN -volume
                ELSE 0 
            END AS obv_delta,
            (close * volume) AS pv_delta
        FROM ",
    table_to_query,
    "
    ),
    periodic_indicators AS (
        SELECT 
            *,
            -- Anchored OBV (resets at the anchor)
            SUM(obv_delta) OVER (PARTITION BY symbol, anchor_group ORDER BY open_time) AS anchored_obv,
            -- Anchored VWAP (resets at the anchor)
            SUM(pv_delta) OVER (PARTITION BY symbol, anchor_group ORDER BY open_time) / 
                NULLIF(SUM(volume) OVER (PARTITION BY symbol, anchor_group ORDER BY open_time), 0) AS anchored_vwap
        FROM base_stats
    )
    SELECT symbol, open_time, close, volume, anchored_vwap, anchored_obv"
  )

  # Part B: Add Dynamic Moving Averages (These do NOT reset; they provide historical context)
  window_funcs <- vapply(
    periods,
    function(p) {
      paste0(
        ", AVG(volume) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
        p - 1,
        " PRECEDING AND CURRENT ROW) AS vol_sma_",
        p,
        ", volume / NULLIF(AVG(volume) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
        p - 1,
        " PRECEDING AND CURRENT ROW), 0) AS vol_rel_",
        p
      )
    },
    character(1)
  )

  full_query <- paste0(
    base_query,
    paste(window_funcs, collapse = ""),
    " FROM periodic_indicators ORDER BY symbol, open_time"
  )

  # 4. Execute
  result <- DBI::dbGetQuery(db_con, full_query)

  return(result)
}

#' Get Keltner Bands
#'
#' @description
#' Calculates Keltner Channels (Upper, Middle, and Lower bands) based on
#' price volatility (ATR) and a central moving average.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. Lookback window for the moving average and ATR (default 20).
#' @param multiplier Numeric. Volatility expansion factor (default 2).
#'
#' @return A data frame containing symbol, open_time, keltner_middle, keltner_upper, and keltner_lower.
#' @export
get_keltner_bands <- function(
  db_con,
  timeframe = "1d",
  period = 20,
  multiplier = 2
) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  if (length(multiplier) != 1 || multiplier <= 0) {
    stop("multiplier must be a positive number.")
  }

  # SQL Query
  query <- paste0(
    "
    WITH raw_ranges AS (
        SELECT
            symbol,
            open_time,
            close,
            GREATEST(
                high - low,
                ABS(high - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)),
                ABS(low - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time))
            ) AS tr
        FROM ",
    table_to_query,
    "
    ),
    stats AS (
        SELECT
            symbol,
            open_time,
            AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS mid,
            AVG(tr) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS atr,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time) AS row_num
        FROM raw_ranges
    )
    SELECT
        symbol,
        open_time,
        mid AS keltner_middle,
        mid + (",
    multiplier,
    " * atr) AS keltner_upper,
        mid - (",
    multiplier,
    " * atr) AS keltner_lower
    FROM stats
    WHERE row_num >= ",
    period,
    "
    ORDER BY symbol, open_time
  "
  )

  return(DBI::dbGetQuery(db_con, query))
}


#' Get Average Directional Index (ADX)
#' @description Calculates +DI, -DI, and ADX to determine trend strength.
#' @param db_con A DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. Lookback window (default 14).
#'
#' @return A data frame containing symbol, open_time, plus_di, minus_di, and adx.
#' @export
get_adx <- function(db_con, timeframe = "1d", period = 14) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # SQL Query: We break down the ADX calculation into multiple CTEs for clarity and correctness.
  query <- paste0(
    "
    WITH raw_dm AS (
        SELECT
            symbol, open_time, high, low,
            GREATEST(high - low, 
                     ABS(high - LAG(close) OVER (PARTITION BY symbol ORDER BY open_time)), 
                     ABS(low - LAG(close) OVER (PARTITION BY symbol ORDER BY open_time))) AS tr,
            high - LAG(high) OVER (PARTITION BY symbol ORDER BY open_time) AS up_move,
            LAG(low) OVER (PARTITION BY symbol ORDER BY open_time) - low AS down_move
        FROM ",
    table_to_query,
    "
    ),
    directional_movement AS (
        SELECT 
            *,
            CASE WHEN up_move > down_move AND up_move > 0 THEN up_move ELSE 0 END AS plus_dm,
            CASE WHEN down_move > up_move AND down_move > 0 THEN down_move ELSE 0 END AS minus_dm
        FROM raw_dm
    ),
    smoothed_stats AS (
        SELECT
            symbol, open_time,
            AVG(tr) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS str,
            AVG(plus_dm) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS s_plus_dm,
            AVG(minus_dm) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS s_minus_dm
        FROM directional_movement
    ),
    di_calc AS (
        SELECT
            symbol, open_time,
            100 * (s_plus_dm / NULLIF(str, 0)) AS plus_di,
            100 * (s_minus_dm / NULLIF(str, 0)) AS minus_di
        FROM smoothed_stats
    ),
    dx_calc AS (
        SELECT
            *,
            100 * (ABS(plus_di - minus_di) / NULLIF(plus_di + minus_di, 0)) AS dx
        FROM di_calc
    )
    SELECT
        symbol, open_time, plus_di, minus_di,
        AVG(dx) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS adx
    FROM dx_calc
    ORDER BY symbol, open_time
  "
  )

  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(row_number() >= period) %>% # Exclude warm-up rows
    ungroup()
  return(result)
}

#' Get Commodity Channel Index (CCI)
#'
#' @description
#' Calculates the CCI based on Typical Price and its Mean Deviation.
#' Commonly used to identify overbought/oversold levels or trend starts.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param period Integer. Lookback window (default 20).
#'
#' @return A data frame containing symbol, open_time, and cci.
#' @export
get_cci <- function(db_con, timeframe = "1d", period = 20) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a single positive integer.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  query <- paste0(
    "
    WITH tp_data AS (
        SELECT
            symbol,
            open_time,
            (high + low + close) / 3 AS tp
        FROM ",
    table_to_query,
    "
    ),
    ma_data AS (
        SELECT
            *,
            AVG(tp) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS avg_tp
        FROM tp_data
    ),
    dev_data AS (
        SELECT
            *,
            ABS(tp - avg_tp) AS absolute_deviation
        FROM ma_data
    )
    SELECT
        symbol,
        open_time,
        (tp - avg_tp) / NULLIF(0.015 * AVG(absolute_deviation) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW), 0) AS cci
    FROM dev_data
    ORDER BY symbol, open_time
  "
  )

  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(row_number() >= period) %>% # Exclude warm-up rows
    ungroup()
  return(result)
}

#' Get Ichimoku Cloud Indicators
#'
#' @description
#' Calculates all five components of the Ichimoku Kinko Hyo.
#' Note: Senkou Spans are shifted back to the current row to represent
#' the active cloud levels for strategy testing.
#'
#' @param db_con A valid DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param tenkan_period Integer (Default 9).
#' @param kijun_period Integer (Default 26).
#' @param senkou_b_period Integer (Default 52).
#' @param chikou_shift Integer (Default 26). Also used for Chikou Span.
#'
#' @return A data frame containing symbol, open_time, tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b, and chikou_span.
#' @export
get_ichimoku_cloud <- function(
  db_con,
  timeframe = "1d",
  tenkan_period = 9,
  kijun_period = 26,
  senkou_b_period = 52,
  chikou_shift = 26
) {
  # Validations
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # Ensure that the periods are valid
  if (
    !is.numeric(tenkan_period) ||
      tenkan_period <= 0 ||
      !is.numeric(kijun_period) ||
      kijun_period <= 0 ||
      !is.numeric(senkou_b_period) ||
      senkou_b_period <= 0 ||
      !is.numeric(chikou_shift) ||
      chikou_shift <= 0
  ) {
    stop("All periods must be positive integers.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  query <- paste0(
    "
    WITH raw_ranges AS (
        SELECT
            symbol, open_time, close,
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    tenkan_period - 1,
    " PRECEDING AND CURRENT ROW) + 
             MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    tenkan_period - 1,
    " PRECEDING AND CURRENT ROW)) / 2 AS tenkan,
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    kijun_period - 1,
    " PRECEDING AND CURRENT ROW) + 
             MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    kijun_period - 1,
    " PRECEDING AND CURRENT ROW)) / 2 AS kijun,
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    senkou_b_period - 1,
    " PRECEDING AND CURRENT ROW) + 
             MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    senkou_b_period - 1,
    " PRECEDING AND CURRENT ROW)) / 2 AS span_b_raw
        FROM ",
    table_to_query,
    "
    ),
    ichimoku_base AS (
        SELECT 
            *,
            (tenkan + kijun) / 2 AS span_a_raw,
            -- Chikou Span is the current close projected back 26 periods
            LEAD(close, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS chikou_span
        FROM raw_ranges
    )
    SELECT
        symbol,
        open_time,
        tenkan AS tenkan_sen,
        kijun AS kijun_sen,
        -- The Cloud: We LAG the raw spans to see the cloud active AT THIS MOMENT
        LAG(span_a_raw, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS senkou_span_a,
        LAG(span_b_raw, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS senkou_span_b,
        chikou_span
    FROM ichimoku_base
    ORDER BY symbol, open_time
  "
  )

  # Execute the query
  result <- DBI::dbGetQuery(db_con, query) %>%
    group_by(symbol) %>%
    filter(
      row_number() >
        min(tenkan_period, kijun_period, senkou_b_period) + chikou_shift
    ) %>% # Exclude warm-up rows
    ungroup()

  # Return the result
  return(result)
}

#' Get Future Price Moves (ML Targets)
#' @description Calculates future max rise, max drop, and net change for specified windows.
#' @param db_con A DBI connection object.
#' @param timeframe Character ("1d" or "1h").
#' @param periods Numeric vector of forward-looking windows (e.g., c(1, 5, 10)).
get_price_moves <- function(db_con, timeframe = "1d", periods = c(1, 5, 10)) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  if (!is.numeric(periods) || any(periods <= 0) || length(periods) == 0) {
    stop("'periods' must be a vector of positive integers.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # Build columns for the CTE
  future_cols <- vapply(
    periods,
    function(p) {
      paste0(
        "MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
        p,
        " FOLLOWING) AS f_max_",
        p,
        ", ",
        "MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
        p,
        " FOLLOWING) AS f_min_",
        p,
        ", ",
        "LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
        p,
        " FOLLOWING) AS f_close_",
        p
      )
    },
    character(1)
  )

  # Build the calculations for the final SELECT
  calc_cols <- vapply(
    periods,
    function(p) {
      paste0(
        "(f_max_",
        p,
        " - close) / NULLIF(close, 0) * 100 AS target_max_rise_",
        p,
        ", ",
        "(f_min_",
        p,
        " - close) / NULLIF(close, 0) * 100 AS target_max_drop_",
        p,
        ", ",
        "(f_close_",
        p,
        " - close) / NULLIF(close, 0) * 100 AS target_net_change_",
        p
      )
    },
    character(1)
  )

  # Build the WHERE clause to clean up the 'tail' of the data
  filter_clauses <- paste0(
    "f_max_",
    periods,
    " IS NOT NULL",
    collapse = " AND "
  )

  query <- paste0(
    "
    WITH future_data AS (
        SELECT symbol, open_time, close,
        ",
    paste(future_cols, collapse = ",\n        "),
    "
        FROM ",
    table_to_query,
    "
    )
    SELECT symbol, open_time,
    ",
    paste(calc_cols, collapse = ",\n    "),
    "
    FROM future_data
    WHERE ",
    filter_clauses,
    "
    ORDER BY symbol, open_time
  "
  )

  return(DBI::dbGetQuery(db_con, query))
}

# # Get the earnings calendar
get_earnings_calendar <- function(db_con) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }
  # 2. SQL Query to get the earnings calendar with period labels and end dates
  earnings_calendar <- dbGetQuery(
    db_con,
    "SELECT 
            act_symbol AS symbol,
            date,
            IF(\"when\" = 'Before market open', TRUE, FALSE) AS before_open,
            IF(\"when\" = 'After market close', TRUE, FALSE) AS after_close,
            -- Create period labels
            CONCAT(EXTRACT(YEAR FROM date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM date - INTERVAL 63 DAY)) AS period_label,
            -- Calculate period_end_date as the last day of the quarter
            DATE_TRUNC('QUARTER', date - INTERVAL 63 DAY) + INTERVAL '3 MONTH' - INTERVAL '1 DAY' AS period_end_date
        FROM 
            earnings_calendar
        WHERE 
            act_symbol IS NOT NULL
        ORDER BY 
            symbol, date;"
  ) %>%
    distinct()

  return(earnings_calendar)
}

# get earnings estimates
get_eps_estimates <- function(
  db_con,
  symbol = NULL,
  start_date = NULL,
  end_date = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  # Base SQL query
  sql_query <- "
    WITH eps_data AS (
      SELECT
          act_symbol,
          period_end_date,
          period,
          date AS estimation_date,
          consensus AS eps_estimated_consensus,
          recent AS eps_estimated_recent,
          count AS eps_estimated_count,
          high AS eps_estimated_high,
          low AS eps_estimated_low,
          year_ago AS eps_year_ago,
          -- Create period labels
          CASE
              WHEN period LIKE '%Quarter' THEN CONCAT(EXTRACT(YEAR FROM period_end_date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM period_end_date - INTERVAL 63 DAY))
              WHEN period LIKE '%Year' THEN CAST(EXTRACT(YEAR FROM period_end_date - INTERVAL 63 DAY) AS VARCHAR)
          END AS period_label
      FROM eps_estimate
      WHERE 1 = 1
  "

  # Add date filtering if start_date or end_date is provided
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND period_end_date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND period_end_date <= '", end_date, "'")
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
    eps_comparison AS (
      SELECT
          *,
          -- Get the previous eps_estimated_consensus for the same symbol and period type
          LAG(eps_estimated_consensus) OVER (PARTITION BY act_symbol, period ORDER BY estimation_date) AS previous_eps_estimated_consensus
      FROM eps_data
    )
    SELECT
        act_symbol,
        period_end_date,
        period,
        estimation_date,
        eps_estimated_consensus,
        eps_estimated_count,
        eps_estimated_high,
        eps_estimated_low,
        eps_year_ago,
        period_label,
        -- Compare with the previous eps_estimated_consensus
        CASE
            WHEN eps_estimated_consensus > previous_eps_estimated_consensus THEN TRUE
            ELSE FALSE
        END AS eps_consensus_grew,
        CASE
            WHEN eps_estimated_consensus < previous_eps_estimated_consensus THEN TRUE
            ELSE FALSE
        END AS eps_consensus_reduced
    FROM eps_comparison
    ORDER BY act_symbol, estimation_date, period_end_date;
  "
  )

  # Execute the query
  eps_estimates <- dbGetQuery(db_con, sql_query) %>%
    distinct()

  return(eps_estimates)
}

# get earnings history
get_eps_history <- function(
  db_con,
  symbol = NULL,
  start_date = NULL,
  end_date = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  # Base SQL query
  sql_query <- "WITH eps_history_data AS (
                  SELECT
                      act_symbol,
                      period_end_date,
                      reported,
                      estimate,
                      -- Create period labels
                      CONCAT(EXTRACT(YEAR FROM period_end_date - INTERVAL 63 DAY), '-Q', EXTRACT(QUARTER FROM period_end_date - INTERVAL 63 DAY)) AS period_label
                  FROM eps_history
                  WHERE 1 = 1"

  # Add date filtering if start_date or end_date is provided
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND period_end_date >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND period_end_date <= '", end_date, "'")
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
    eps_comparison AS (
      SELECT
          *,
          -- Get the previous reported and estimate values for the same symbol and period type
          LAG(reported) OVER (PARTITION BY act_symbol ORDER BY period_end_date) AS previous_reported,
          LAG(estimate) OVER (PARTITION BY act_symbol ORDER BY period_end_date) AS previous_estimate
      FROM eps_history_data
    )
    SELECT
        act_symbol,
        period_end_date,
        reported AS eps_reported,
        estimate AS eps_estimated,
        period_label
    FROM eps_comparison
    ORDER BY act_symbol, period_end_date;
  "
  )

  # Execute the query
  eps_history <- dbGetQuery(db_con, sql_query) %>%
    distinct()

  eps_history <- eps_history %>%
    rename(symbol = act_symbol) %>%
    group_by(symbol) %>%
    arrange(period_end_date) %>%
    mutate(
      # Handle division by zero in surprise calculations
      surprise_amount = eps_reported - eps_estimated,
      surprise_percent = ifelse(
        abs(eps_estimated) < 1e-10,
        NA_real_,
        (eps_reported - eps_estimated) / abs(eps_estimated) * 100
      ),

      # Growth calculations with NA handling
      yoy_growth = ifelse(
        abs(lag(eps_reported, 4)) < 1e-10 | is.na(lag(eps_reported, 4)),
        NA_real_,
        (eps_reported - lag(eps_reported, 4)) / abs(lag(eps_reported, 4)) * 100
      ),

      sequential_growth = ifelse(
        abs(lag(eps_reported)) < 1e-10 | is.na(lag(eps_reported)),
        NA_real_,
        (eps_reported - lag(eps_reported)) / abs(lag(eps_reported)) * 100
      ),
      # Robust consecutive beats calculation
      beat = ifelse(
        is.na(eps_reported) | is.na(eps_estimated),
        FALSE,
        eps_reported > eps_estimated
      ),
      run_length = sequence(rle(beat)$lengths),
      consecutive_beats = ifelse(beat, run_length, 0),

      # Consistent consecutive growth calculation
      positive_growth = ifelse(
        is.na(sequential_growth),
        FALSE,
        sequential_growth > 0 & !is.infinite(sequential_growth)
      ),
      growth_run_length = sequence(rle(positive_growth)$lengths),
      consecutive_growth = ifelse(positive_growth, growth_run_length, 0)
    ) %>%
    select(-beat, -run_length, -positive_growth, -growth_run_length) # Clean up temp columns

  return(eps_history)
}

# get the earnings history for a specific symbol
get_eps_data <- function(
  db_con,
  symbols_price_data
  # , fake_Q3_2019 = FALSE
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  symbol <- symbols_price_data$symbol %>%
    unique()

  # load both data
  earnings_calendar <- get_earnings_calendar(db_con) %>%
    filter(symbol %in% symbol) %>%
    mutate(
      open_time = case_when(
        before_open ~ date, # AM earnings affect same day
        after_close ~ date + 1, # PM earnings affect next day
        TRUE ~ date + 1 # Default to conservative
      )
    ) %>%
    select(-date)

  eps_history <- get_eps_history(db_con, symbol) %>%
    select(-period_end_date) %>%
    filter(symbol %in% symbol)

  eps_estimates <- get_eps_estimates(db_con, symbol)

  eps_estimates_quarters <- eps_estimates %>%
    filter(grepl("Q", period_label))

  eps_estimates_year <- eps_estimates %>%
    filter(!grepl("Q", period_label))

  new_data <- create_earnings_features(
    symbols_price_data,
    earnings_calendar,
    eps_history,
    eps_estimates_quarters
  )

  alert_1_eps_source <- "- This EPS data was downloaded as csv from https://www.dolthub.com/repositories/post-no-preference/earnings and saved on the database on Z:/stock_data/stock_data.duckdb 
  It may need some checks. Use https://finance.yahoo.com/calendar/earnings to check on earning calendars if needed"
  message(alert_1_eps_source)

  alert_2_eps_history <- "- The column 'Quarter' on eps_history is calculated and may be wrong. Double check it if needed"
  message(alert_2_eps_history)

  alert_3_calendar <- "- The earnings_calendar data have some missing data and conflicts with data from yahoo finance."
  message(alert_3_calendar)

  # Return the results
  return(new_data)
}

# Fix specific problematic columns in existing data
fix_financial_infinities <- function(eps_data) {
  # Define winsorization limits - adjust these based on your data distribution
  upper_limit <- 1000 # Cap very large positive values (e.g., 1000%)
  lower_limit <- -1000 # Cap very large negative values

  eps_data %>%
    mutate(
      # Handle consensus_change_percent - Winsorize extreme values
      consensus_change_percent = case_when(
        is.infinite(consensus_change_percent) & consensus_change_percent > 0 ~
          upper_limit,
        is.infinite(consensus_change_percent) & consensus_change_percent < 0 ~
          lower_limit,
        !is.infinite(consensus_change_percent) ~ consensus_change_percent
      ),

      # Handle dispersion_ratio - Typically non-negative, cap at reasonable maximum
      dispersion_ratio = case_when(
        is.infinite(dispersion_ratio) ~ 10, # Cap at 10 (extremely high dispersion)
        !is.infinite(dispersion_ratio) ~ dispersion_ratio
      ),

      # Handle surprise_to_dispersion - Can be positive or negative
      surprise_to_dispersion = case_when(
        is.infinite(surprise_to_dispersion) & surprise_to_dispersion > 0 ~
          upper_limit,
        is.infinite(surprise_to_dispersion) & surprise_to_dispersion < 0 ~
          lower_limit,
        !is.infinite(surprise_to_dispersion) ~ surprise_to_dispersion
      ),

      # Handle high_uncertainty_surprise - Typically non-negative
      high_uncertainty_surprise = case_when(
        is.infinite(high_uncertainty_surprise) ~ upper_limit,
        !is.infinite(high_uncertainty_surprise) ~ high_uncertainty_surprise
      )
    )
}

# Function to join everything
create_earnings_features <- function(
  symbols_price_data,
  earnings_calendar,
  eps_history,
  eps_estimates_quarters
  # fake_Q3_2019 = NULL
) {
  # 1. Process earnings data
  eps_data <- earnings_calendar %>%
    left_join(eps_history, by = c("symbol", "period_label")) %>%
    # rename(open_time = date) %>%
    # {
    #   if (!is.null(fake_Q3_2019)) bind_rows(., fake_Q3_2019) else .
    # } %>%
    mutate(is_earning_day = TRUE) %>%
    arrange(symbol, open_time)

  # 2. Process consensus estimates
  consensus_changes_current <- eps_estimates_quarters %>%
    filter(grepl("current", tolower(period))) %>%
    rename(symbol = act_symbol) %>%
    group_by(symbol) %>%
    arrange(estimation_date) %>%
    mutate(
      is_estimation_day = TRUE,
      consensus_change = eps_estimated_consensus - lag(eps_estimated_consensus),
      consensus_change_percent = consensus_change /
        lag(eps_estimated_consensus) *
        100,
      estimate_range = eps_estimated_high - eps_estimated_low,
      dispersion_ratio = estimate_range / eps_estimated_consensus,
      open_time = estimation_date + days(1) # Adjust open_time to the next day, since most estimates are released on weekends
    ) %>%
    ungroup() %>%
    select(-period_end_date)

  # 3. Join everything to price data in ONE operation
  result <- symbols_price_data %>%
    # Left join earnings data
    left_join(eps_data, by = c("symbol", "open_time")) %>%
    # Left join consensus data
    left_join(
      consensus_changes_current,
      by = c("symbol", "open_time"),
      suffix = c("", "_consensus")
    ) %>%
    select(
      -c(
        period_label,
        period_end_date,
        period,
        estimation_date,
        period_label_consensus
      )
    )

  # 4. Fill forward values efficiently by group
  result <- result %>%
    group_by(symbol) %>%
    arrange(symbol, open_time) %>%
    mutate(
      # Set flags properly
      is_earning_day = ifelse(is.na(is_earning_day), FALSE, TRUE),
      is_estimation_day = ifelse(is.na(is_estimation_day), FALSE, TRUE),
      # Fill forward earnings data
      across(
        c(
          eps_reported,
          eps_estimated,
          surprise_amount,
          surprise_percent,
          yoy_growth,
          sequential_growth,
          # consistent_beats,
          consecutive_beats
        ),
        ~ zoo::na.locf(.x, na.rm = FALSE)
      ),

      # Fill forward estimate data
      across(
        c(
          eps_estimated_consensus,
          eps_estimated_count,
          eps_estimated_high,
          eps_estimated_low,
          eps_year_ago,
          eps_consensus_grew,
          eps_consensus_reduced,
          consensus_change,
          consensus_change_percent,
          estimate_range,
          dispersion_ratio
        ),
        ~ zoo::na.locf(.x, na.rm = FALSE)
      ),

      # Create temporal features
      last_earnings_date = zoo::na.locf(
        if_else(is_earning_day, open_time, NA),
        na.rm = FALSE
      ),
      next_earnings_date = zoo::na.locf(
        if_else(is_earning_day, open_time, NA),
        na.rm = FALSE,
        fromLast = TRUE
      )
    ) %>%
    # Calculate derived temporal features
    mutate(
      # Days since/until earnings
      days_since_last_earnings = as.numeric(difftime(
        open_time,
        last_earnings_date,
        units = "days"
      )),
      days_to_next_earnings = as.numeric(difftime(
        next_earnings_date,
        open_time,
        units = "days"
      )),

      # Earnings proximity flags
      in_earnings_week = ifelse(
        days_to_next_earnings <= 5 & !is.na(days_to_next_earnings),
        TRUE,
        FALSE
      ),
      post_earnings_week = ifelse(
        days_since_last_earnings <= 5 & !is.na(days_since_last_earnings),
        TRUE,
        FALSE
      ),

      # Earnings seasonality features
      quarter = quarter(open_time),
      is_quarterly_earnings_season = month(open_time) %in% c(1, 4, 7, 10),

      # Standardized time features
      earnings_cycle_position = ifelse(
        !is.na(days_since_last_earnings) & !is.na(days_to_next_earnings),
        days_since_last_earnings /
          (days_since_last_earnings + days_to_next_earnings),
        NA
      ),

      # Create combined features between earnings and estimates
      surprise_to_dispersion = ifelse(
        !is.na(surprise_percent) & !is.na(dispersion_ratio),
        surprise_percent / dispersion_ratio,
        NA
      ),
      high_uncertainty_surprise = ifelse(
        !is.na(surprise_percent) & !is.na(dispersion_ratio),
        abs(surprise_percent) * dispersion_ratio,
        NA
      ),

      # Additional feature: Expected earnings impact
      expected_eps_growth = ifelse(
        !is.na(eps_estimated_consensus) & !is.na(eps_year_ago),
        (eps_estimated_consensus - eps_year_ago) / abs(eps_year_ago),
        NA
      )
    ) %>%
    ungroup()

  # 5. Select and order columns

  result <- result %>%
    select(
      symbol,
      open_time,
      # Earnings day indicators
      is_earning_day,
      is_estimation_day,
      # Temporal features
      days_since_last_earnings,
      days_to_next_earnings,
      in_earnings_week,
      post_earnings_week,
      is_quarterly_earnings_season,
      earnings_cycle_position,
      # Earnings data
      eps_reported,
      surprise_amount,
      surprise_percent,
      yoy_growth,
      sequential_growth,
      # consistent_beats,
      consecutive_beats,
      # Estimates data
      eps_estimated_consensus,
      eps_estimated_count,
      eps_estimated_high,
      eps_estimated_low,
      eps_year_ago,
      eps_consensus_grew,
      eps_consensus_reduced,
      consensus_change,
      consensus_change_percent,
      estimate_range,
      dispersion_ratio,
      # Combined features
      surprise_to_dispersion,
      high_uncertainty_surprise,
      expected_eps_growth,
      # Keep any other columns from price data
      everything()
    ) %>%
    select(-c(last_earnings_date, next_earnings_date))

  return(result)
}

# get the xgb model as an indicator
get_xgb_indicator <- function(model_labels) {
  # Connect to the model database
  db_name <- "models_data.duckdb"
  # check if I'm running in windows or linux
  if (.Platform$OS.type == "windows") {
    db_path <- file.path("Z:/stock_data", db_name)
  } else {
    db_path <- file.path("/mnt/nas_nuvens/stock_data", db_name)
  }

  # Database db_con
  model_conn <- dbConnect(
    duckdb(),
    dbdir = db_path,
    read_only = FALSE
  )

  model_labels <- paste0("'", model_labels, "'", collapse = ", ")

  # Query the data
  sql_query <- paste0(
    "SELECT * 
                      FROM xgb_indicator
                      WHERE model_label IN (",
    model_labels,
    ")"
  )

  # load the data
  xgb_indicator <- dbGetQuery(model_conn, sql_query)

  # disconnect from the model database
  dbDisconnect(model_conn)

  return(xgb_indicator)
}


# Check the high before the low for TP ans SL filters
check_high_before_low <- function(
  price_data,
  high_threshold = 3,
  low_threshold = -3,
  max_period = 5
) {
  # Ensure data is properly sorted
  price_data <- price_data %>%
    arrange(symbol, open_time) %>%
    group_by(symbol) %>%
    mutate(
      # Create rolling windows for future highs and lows
      future_highs = purrr::map(
        row_number(),
        ~ if (.x + max_period <= n()) {
          high[.x:(.x + max_period - 1)]
        } else {
          high[.x:n()]
        }
      ),
      future_lows = purrr::map(
        row_number(),
        ~ if (.x + max_period <= n()) {
          low[.x:(.x + max_period - 1)]
        } else {
          low[.x:n()]
        }
      ),

      # Calculate percentage changes from current close
      high_pct_changes = purrr::map2(
        future_highs,
        close,
        ~ (.x - .y) / .y * 100
      ),
      low_pct_changes = purrr::map2(future_lows, close, ~ (.x - .y) / .y * 100),

      # Check if high threshold is reached before low threshold in each window
      check_high_before_low = purrr::map2_lgl(
        high_pct_changes,
        low_pct_changes,
        ~ {
          # Find first positions where thresholds are crossed
          high_pos = which(.x >= high_threshold)
          low_pos = which(.y <= low_threshold)

          # If neither threshold is hit, return FALSE
          if (length(high_pos) == 0 && length(low_pos) == 0) {
            return(FALSE)
          }

          # If only high is hit, return TRUE
          if (length(high_pos) > 0 && length(low_pos) == 0) {
            return(TRUE)
          }

          # If only low is hit, return FALSE
          if (length(high_pos) == 0 && length(low_pos) > 0) {
            return(FALSE)
          }

          # Compare which threshold was hit first
          min(high_pos) < min(low_pos)
        }
      )
    ) %>%
    select(-future_highs, -future_lows, -high_pct_changes, -low_pct_changes) %>%
    ungroup()

  # Remove auxiliary row_num column
  price_data$row_num <- NULL

  return(price_data)
}


# Function to get benchmarks indicators
# This function loads and processes external market indexes data to use as benchmarks
get_external_indexes <- function(
  db_con,
  timeframe = "1d",
  start_date = NULL,
  end_date = NULL,
  symbols = NULL
) {
  # 1. Validation using your existing connection checker
  if (!is_valid_db_connection(db_con)) {
    stop("db_con must be a valid DBI connection.")
  }

  table_map <- c("1d" = "daily_prices", "1h" = "hourly_prices")

  if (!timeframe %in% names(table_map)) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  table_to_query <- table_map[timeframe]

  # If no symbol is provided, fetch all external market indexes
  if (is.null(symbols)) {
    symbols <- c(
      "^GSPC",
      "^DJI",
      "^IXIC",
      "VT",
      "^TNX",
      "LQD",
      "BNDW",
      "GC=F",
      "GSG",
      "^VIX",
      "DX-Y.NYB",
      "BTC-USD",
      "VNQ",
      "TIP",
      "HYG",
      "JNK"
    )
  }

  # Prepare the list of symbols for the SQL query
  symbols_list <- paste0("'", symbols, "'", collapse = ",")

  # Construct the SQL query to fetch data for the specified symbols
  sql_query <- paste0(
    "SELECT * FROM ",
    table_to_query,
    " WHERE symbol IN (",
    symbols_list,
    ")"
  )

  # Add date filtering if start_date or end_date is provided
  if (!is.null(start_date)) {
    sql_query <- paste0(sql_query, " AND open_time >= '", start_date, "'")
  }
  if (!is.null(end_date)) {
    sql_query <- paste0(sql_query, " AND open_time <= '", end_date, "'")
  }

  # Execute the query and process the data
  external_indexes_data <- dbGetQuery(db_con, sql_query) %>%
    dplyr::select(symbol, open_time, close) %>%
    tidyr::pivot_wider(
      names_from = symbol,
      values_from = close
    )

  # Return the processed data
  return(external_indexes_data)
}
