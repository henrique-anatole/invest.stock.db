#' Get the Simple Moving Averages (SMA) for a given set of periods
get_smas <- function(
  db_con,
  timeframe = "1d",
  periods = c(20, 50, 150, 200)
) {
  # Ensure that the 'periods' parameter is valid
  if (!is.numeric(periods) || length(periods) == 0 || any(periods <= 0)) {
    stop("'periods' must be a vector of positive integers.")
  }
  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }
  # Ensure that the db_con object is valid
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    # "1w" = "weekly_prices",
    # "1m" = "monthly_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- "SELECT symbol, open_time"

  # Loop over each period and add the corresponding AVG window function to the query
  for (period in periods) {
    # Dynamically generate the SQL for each period
    select_query <- paste0(
      select_query,
      ", AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
      (period - 1),
      " PRECEDING AND CURRENT ROW) AS SMA_",
      period
    )
  }

  # Add the FROM clause
  select_query <- paste0(select_query, " FROM ", table_to_query)

  # Execute the query and return the results as a data frame
  result <- dbGetQuery(db_con, select_query)

  # Exclude the first rows where there is insufficient data to calculate the averages
  # result <- result %>%
  #   group_by(symbol) %>%
  #   filter(row_number() > max(periods)) %>%
  #   ungroup()

  return(result)
}

# Function to calculate EMA for a single period
calculate_single_ema <- function(data, period, column_name = "close") {
  # Handle NA values in the specified column
  data <- data %>%
    mutate(!!column_name := na.locf(get(column_name), na.rm = FALSE)) # Carry forward last observation

  if (nrow(data) < period || all(is.na(data[[column_name]]))) {
    # If there are insufficient rows or all values are NA, return NA for the EMA
    return(rep(NA, nrow(data)))
  } else {
    # Calculate EMA using TTR::EMA
    return(TTR::EMA(data[[column_name]], n = period))
  }
}

#' Get the Exponential Moving Averages (EMA) for a given set of periods
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
  if (length(periods) == 0) {
    stop("At least one period must be specified.")
  }

  # Sort data by 'open_time' before calculation
  timeseries <- timeseries %>%
    arrange(symbol, open_time)

  # Loop over all provided periods and calculate EMA for each
  for (period in periods) {
    # Create a new column name for the EMA
    ema_col_name <- paste0("ema_", period)

    # Calculate EMA for the current period and add it to the dataframe
    timeseries <- timeseries %>%
      group_by(symbol) %>%
      mutate(!!ema_col_name := calculate_single_ema(cur_data(), period)) %>%
      ungroup()
  }

  # Exclude the first rows where there is insufficient data to calculate the averages
  # timeseries <- timeseries %>%
  #   group_by(symbol) %>%
  #   filter(row_number() > max(periods)) %>%
  #   ungroup()
  #
  # Return the timeseries with the new columns
  return(timeseries)
}
# daily_data = dbGetQuery(db_con, "SELECT * FROM daily_prices")
# emas = get_emas(daily_data, c(20, 50, 150, 200))

# Define the function to get Bollinger Bands with the dynamic timeframe
get_bollinger_bands <- function(db_con, timeframe = "1d", period = 20) {
  # Ensure that the 'period' is specified and valid
  if (length(period) != 1) {
    stop("Only one period is allowed.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- "SELECT symbol, open_time"

  # Calculate the SMA for the given period
  select_query <- paste0(
    select_query,
    ", AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS SMA_",
    period
  )

  # Calculate the Standard Deviation for the given period
  select_query <- paste0(
    select_query,
    ", STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS STDDEV_",
    period
  )

  # Calculate the Upper and Lower Bollinger Bands
  select_query <- paste0(
    select_query,
    ", AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) + (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Upper_Band_",
    period
  )

  select_query <- paste0(
    select_query,
    ", AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) - (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Lower_Band_",
    period
  )

  # calculate the distance of the close price to the upper band
  select_query <- paste0(
    select_query,
    ", (close - (AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) + (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW))) ) / (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Distance_Upper_Band_",
    period
  )

  # calculate the distance of the close price to the lower band
  select_query <- paste0(
    select_query,
    ", (close - (AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) - (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW))) ) / (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Distance_Lower_Band_",
    period
  )

  # calculate the distance of the high price to the upper band
  select_query <- paste0(
    select_query,
    ", (high - (AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) + (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW))) ) / (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Distance_High_Upper_Band_",
    period
  )

  # calculate the distance of the low price to the lower band
  select_query <- paste0(
    select_query,
    ", (low - (AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) - (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW))) ) / (2 * STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS Distance_Low_Lower_Band_",
    period
  )

  # Add the WHERE clause to avoid filtering out rows before SQL execution
  select_query <- paste0(
    select_query,
    " FROM ",
    table_to_query,
    " ORDER BY symbol, open_time"
  )

  # Execute the query and return the results as a data frame
  result <- dbGetQuery(db_con, select_query)

  # Exclude the first rows where there is insufficient data to calculate Bollinger Bands
  result <- result %>%
    group_by(symbol) %>%
    filter(row_number() > period) %>%
    ungroup()

  return(result)
}

# Define the MACD function using `get_ema`
get_macd <- function(
  timeseries,
  fast_period = 12,
  slow_period = 26,
  signal_period = 9,
  maType = "EMA",
  percent = TRUE
) {
  # Ensure the timeseries has necessary columns
  if (!"symbol" %in% colnames(timeseries)) {
    stop("The timeseries must have a 'symbol' column.")
  }
  if (!"open_time" %in% colnames(timeseries)) {
    stop("The timeseries must have an 'open_time' column.")
  }

  # Check for the quantity of rows of each symbol in the timeseries
  symbol_counts <- timeseries %>%
    group_by(symbol) %>%
    summarise(n = n()) %>%
    ungroup()

  # Filter the symbols with less rows than the slow_period
  symbols_to_filter <- symbol_counts %>%
    filter(n < slow_period) %>%
    pull(symbol)

  # Calculate the Signal line (9-period EMA of the MACD)
  timeseries <- timeseries %>%
    group_by(symbol) %>%
    na.locf() %>%
    # filter out any symbol in the list of symbols to filter
    filter(!symbol %in% symbols_to_filter) %>%
    mutate(
      macd = MACD(
        close,
        nFast = fast_period,
        nSlow = slow_period,
        nSig = signal_period,
        maType = maType,
        percent = percent
      )[, 1],
      signal_line = MACD(
        close,
        nFast = fast_period,
        nSlow = slow_period,
        nSig = signal_period,
        maType = maType,
        percent = percent
      )[, 2],
      macd_histogram = macd - signal_line,
      macd_direction = ifelse(macd_histogram > 0, 1, 0)
    ) %>%
    ungroup() %>%
    filter(!is.na(signal_line))

  # Return the updated timeseries with MACD and Signal Line
  return(timeseries)
}

# Get the main indicators related to volatility
get_volatilities <- function(
  db_con,
  timeframe = "1d",
  periods = c(10, 20, 50)
) {
  # Ensure that the 'periods' are valid
  if (!is.numeric(periods) || length(periods) == 0 || any(periods <= 0)) {
    stop("'periods' must be a vector of positive integers.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            open,
            high,
            low,
            close,
            -- Calculate intraday volatility (high - low)
            high - low AS intraday_volatility,
            -- Calculate true range
            GREATEST(
                high - low,
                ABS(high - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)),
                ABS(low - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time))
            ) AS true_range,
            -- Calculate gap down
            CASE WHEN open < LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 THEN (open - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 ELSE NULL END AS gap_down,
            -- Calculate gap up
            CASE WHEN open > LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 THEN (open - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)
                 ELSE NULL END AS gap_up
        FROM ",
    table_to_query,
    "
    )
    SELECT
        symbol,
        open_time,
        intraday_volatility,
        true_range,
        gap_down,
        gap_up"
  )

  # Add volatility and ATR calculations for each period
  for (period in periods) {
    select_query <- paste0(
      select_query,
      ", STDDEV_POP(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
      (period - 1),
      " PRECEDING AND CURRENT ROW) AS volatility_",
      period,
      ", AVG(true_range) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
      (period - 1),
      " PRECEDING AND CURRENT ROW) AS atr_",
      period
    )
  }

  # Add the FROM clause
  select_query <- paste0(select_query, " FROM price_data")

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Check for recent gaps in most recent periods
check_recent_gaps <- function(timeseries, gap_size = 0.03, periods = c(5)) {
  # Check if the timeseries has the necessary columns (gap_up or gap_down)
  if (!("gap_up" %in% colnames(timeseries))) {
    stop(
      "The timeseries must have a 'gap_up' and a 'gap_down' column. Run get_volatilies(db_con) to get these columns."
    )
  }

  # Creates the new column, checking if any of the gap_up or gap_down columns are bigger than the gap_size in the previous periods

  timeseries <- timeseries %>%
    group_by(symbol) %>%
    mutate(
      significant_gap = coalesce(gap_up, abs(gap_down), 0) > gap_size,
      # Check for significant gaps in the most recent `periods` rows
      recent_periods_gap = slider::slide_lgl(
        significant_gap,
        .f = ~ any(.x, na.rm = TRUE),
        .before = periods - 1, # Look back `periods - 1` rows (including the current row)
        .complete = TRUE # Only evaluate when there are enough rows in the window
      )
    ) %>%
    ungroup()

  return(timeseries)
}

# Get the Min and Max values for a given period
get_min_max <- function(db_con, timeframe = "1d", period = 52) {
  # Ensure that the 'period' is specified and valid
  if (length(period) != 1) {
    stop("Only one period is allowed.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "SELECT symbol, open_time, ",
    "MIN(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS min_",
    period,
    ",",
    "MAX(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS max_",
    period,
    ",",
    "FROM ",
    table_to_query
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get the Pivot Points for a given period
get_pivots <- function(db_con, weekly = TRUE, fib = FALSE, round = FALSE) {
  weekly_fib_query <- "WITH weekly_data AS (
                  -- Aggregate daily data into weekly data, considering the year
                  SELECT
                  symbol,
                  DATE_TRUNC('week', open_time) AS week_start,
                  EXTRACT(YEAR FROM open_time) AS year,  -- Extract the year
                  MAX(high) AS weekly_high,
                  MIN(low) AS weekly_low,
                  ANY_VALUE(close) AS weekly_close  -- Use ANY_VALUE to get a single close value
                  FROM
                  daily_prices
                  GROUP BY
                  symbol,
                  DATE_TRUNC('week', open_time),
                  EXTRACT(YEAR FROM open_time)  -- Group by year
                ),
                weekly_pivots AS (
                  -- Calculate Fibonacci pivots on weekly data, considering the year
                  SELECT
                  symbol,
                  week_start,
                  year,
                  weekly_high,
                  weekly_low,
                  weekly_close,
                  -- Calculate the pivot point
                  (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) +
                      LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) +
                      LAG(weekly_close, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) / 3 AS pivt,
                  -- Calculate Fibonacci resistance levels (R1, R2, R3)
                  pivt + (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 0.382 AS r1,
                  pivt + (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 0.618 AS r2,
                  pivt + (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 1.000 AS r3,
                  -- Calculate Fibonacci support levels (S1, S2, S3)
                  pivt - (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 0.382 AS s1,
                  pivt - (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 0.618 AS s2,
                  pivt - (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                            LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) * 1.000 AS s3,
                  -- Swing high (previous week's high)
                      LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) AS swing_high
                  FROM
                      weekly_data
              )
              -- Join weekly pivots back to daily data
              SELECT
                  d.symbol,
                  d.open_time,
                  d.high AS daily_high,
                  d.low AS daily_low,
                  d.close AS daily_close,
                  w.pivt,
                  w.r1,
                  w.r2,
                  w.r3,
                  w.s1,
                  w.s2,
                  w.s3,
                  w.swing_high
              FROM
                  daily_prices d
              LEFT JOIN
                  weekly_pivots w
              ON
                  d.symbol = w.symbol
                  AND DATE_TRUNC('week', d.open_time) = w.week_start
                  AND EXTRACT(YEAR FROM d.open_time) = w.year  -- Join on year
              ORDER BY
                  d.symbol,
                  d.open_time;"

  weekly_reg_query <- "WITH weekly_data AS (
              -- Aggregate daily data into weekly data, considering the year
              SELECT
                  symbol,
                  DATE_TRUNC('week', open_time) AS week_start,
                  EXTRACT(YEAR FROM open_time) AS year,  -- Extract the year
                  MAX(high) AS weekly_high,
                  MIN(low) AS weekly_low,
                  ANY_VALUE(close) AS weekly_close  -- Use ANY_VALUE to get a single close value
              FROM
                  daily_prices
              GROUP BY
                  symbol,
                  DATE_TRUNC('week', open_time),
                  EXTRACT(YEAR FROM open_time)  -- Group by year
          ),
          weekly_pivots AS (
              -- Calculate regular pivots on weekly data, considering the year
              SELECT
                  symbol,
                  week_start,
                  year,
                  weekly_high,
                  weekly_low,
                  weekly_close,
                  -- Calculate the pivot point
                  (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) +
                   LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) +
                   LAG(weekly_close, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) / 3 AS pivt,
                  -- Calculate resistance levels (R1, R2, R3)
                  (2 * pivt) - LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) AS r1,
                  pivt + (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                          LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) AS r2,
                  LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) +
                  2 * (pivt - LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) AS r3,
                  -- Calculate support levels (S1, S2, S3)
                  (2 * pivt) - LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) AS s1,
                  pivt - (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                          LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start)) AS s2,
                  LAG(weekly_low, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) -
                  2 * (LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) - pivt) AS s3,
                  -- Swing high (previous week's high)
                  LAG(weekly_high, 1) OVER (PARTITION BY symbol, year ORDER BY week_start) AS swing_high
              FROM
                  weekly_data
          )
          -- Join weekly pivots back to daily data
          SELECT
              d.symbol,
              d.open_time,
              d.high AS daily_high,
              d.low AS daily_low,
              d.close AS daily_close,
              w.pivt,
              w.r1,
              w.r2,
              w.r3,
              w.s1,
              w.s2,
              w.s3,
              w.swing_high
          FROM
              daily_prices d
          LEFT JOIN
              weekly_pivots w
          ON
              d.symbol = w.symbol
              AND DATE_TRUNC('week', d.open_time) = w.week_start
              AND EXTRACT(YEAR FROM d.open_time) = w.year  -- Join on year
          ORDER BY
              d.symbol,
              d.open_time;"

  daily_fib_query <- "WITH pivot_data AS (
                      SELECT
                          symbol,
                          open_time,
                          high,
                          low,
                          close,
                          -- Calculate the pivot point
                          (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) +
                           LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) +
                           LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / 3 AS pivt,
                          -- Calculate Fibonacci resistance levels (R1, R2, R3)
                          pivt + (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 0.382 AS r1,
                          pivt + (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 0.618 AS r2,
                          pivt + (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 1.000 AS r3,
                          -- Calculate Fibonacci support levels (S1, S2, S3)
                          pivt - (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 0.382 AS s1,
                          pivt - (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 0.618 AS s2,
                          pivt - (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) * 1.000 AS s3,
                          -- Swing high (previous day's high)
                          LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS swing_high
                      FROM
                          daily_prices
                   )
                  SELECT
                      symbol,
                      open_time,
                      pivt,
                      r1,
                      r2,
                      r3,
                      s1,
                      s2,
                      s3,
                      swing_high
                      FROM
                          pivot_data
                      WHERE
                          pivt IS NOT NULL  -- Exclude rows where pivot cannot be calculated (e.g., first row)
                      ORDER BY
                          symbol,
                          open_time;"

  daily_reg_query <- "WITH pivot_data AS (
                      SELECT
                          symbol,
                          open_time,
                          high,
                          low,
                          close,
                          -- Calculate the pivot point
                          (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) +
                           LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) +
                           LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) / 3 AS pivt,
                          -- Calculate resistance levels (R1, R2, R3)
                          (2 * pivt) - LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS r1,
                          pivt + (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) AS r2,
                          LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) +
                          2 * (pivt - LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) AS r3,
                          -- Calculate support levels (S1, S2, S3)
                          (2 * pivt) - LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS s1,
                          pivt - (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                                  LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time)) AS s2,
                          LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) -
                          2 * (LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) - pivt) AS s3,
                          -- Swing high (previous day's high)
                          LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS swing_high
                      FROM
                          daily_prices
                      )
                      SELECT
                          symbol,
                          open_time,
                          pivt,
                          r1,
                          r2,
                          r3,
                          s1,
                          s2,
                          s3,
                          swing_high
                      FROM
                          pivot_data
                      WHERE
                          pivt IS NOT NULL  -- Exclude rows where pivot cannot be calculated (e.g., first row)
                      ORDER BY
                          symbol,
                          open_time;"

  query <- case_when(
    weekly == TRUE & fib == TRUE ~ weekly_fib_query,
    weekly == TRUE & fib == FALSE ~ weekly_reg_query,
    weekly == FALSE & fib == TRUE ~ daily_fib_query,
    weekly == FALSE & fib == FALSE ~ daily_reg_query
  )

  # Execute the query
  result <- DBI::dbGetQuery(db_con, query)

  # Return the result
  return(result)
}

# Get the RSI and Stochastic Oscillator for a given period
get_oscillators <- function(db_con, timeframe = "1d", period = 14) {
  # Ensure that the 'period' is specified and valid
  if (length(period) != 1) {
    stop("Only one period is allowed.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_changes AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate price change from the previous period
            close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS price_change
        FROM
            ",
    table_to_query,
    "
    ),
    gains_losses AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Separate gains and losses
            CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
            CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
        FROM
            price_changes
    ),
    avg_gains_losses AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate average gains and losses over the specified period
            AVG(gain) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS avg_gain,
            AVG(loss) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS avg_loss
        FROM
            gains_losses
    ),
    rsi_data AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate RSI
            100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) AS rsi
        FROM
            avg_gains_losses
    ),
    high_low AS (
        SELECT
            symbol,
            open_time,
            close,
            high,
            low,
            -- Calculate highest high and lowest low over the specified period
            MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS highest_high,
            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period - 1,
    " PRECEDING AND CURRENT ROW) AS lowest_low
        FROM
            ",
    table_to_query,
    "
    ),
    stoch_data AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate Stochastic %K
            (close - lowest_low) / NULLIF((highest_high - lowest_low), 0) * 100 AS stochastic_k
        FROM
            high_low
    )
    SELECT
        r.symbol,
        r.open_time,
        r.rsi,
        s.stochastic_k
    FROM
        rsi_data r
    JOIN
        stoch_data s
    ON
        r.symbol = s.symbol
        AND r.open_time = s.open_time
    ORDER BY
        r.symbol,
        r.open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get various trend-following indicators
get_trend_followers <- function(db_con, timeframe = "1d", period = 14) {
  # Ensure that the 'period' is specified and valid
  if (length(period) != 1 || !is.numeric(period) || period <= 0) {
    stop("'period' must be a positive integer.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate Momentum (current close - close 'period' periods ago)
            close - LAG(close, ",
    period,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS momentum,
            -- Calculate ROC (Rate of Change)
            (close - LAG(close, ",
    period,
    ") OVER (PARTITION BY symbol ORDER BY open_time)) / 
            LAG(close, ",
    period,
    ") OVER (PARTITION BY symbol ORDER BY open_time) * 100 AS roc,
            -- Calculate KER (Kaufman Efficiency Ratio)
            ABS(close - LAG(close, ",
    period,
    ") OVER (PARTITION BY symbol ORDER BY open_time)) AS net_price_change,
            ABS(close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)) AS abs_price_change
        FROM ",
    table_to_query,
    "
    )
    SELECT
        symbol,
        open_time,
        momentum,
        roc,
        net_price_change / NULLIF(SUM(abs_price_change) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    period,
    " PRECEDING AND CURRENT ROW), 0) AS ker
    FROM
        price_data
    WHERE
        momentum IS NOT NULL  -- Exclude rows where momentum cannot be calculated
    ORDER BY
        symbol,
        open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get the Volume Oscillators
get_vol_oscillators <- function(
  db_con,
  timeframe = "1d",
  rsi_period = 14,
  force_index_smoothing = 13
) {
  # Ensure that the 'rsi_period' and 'force_index_smoothing' are valid
  if (!is.numeric(rsi_period) || rsi_period <= 0 || length(rsi_period) != 1) {
    stop("'rsi_period' must be a positive integer.")
  }
  if (
    !is.numeric(force_index_smoothing) ||
      force_index_smoothing <= 0 ||
      length(force_index_smoothing) != 1
  ) {
    stop("'force_index_smoothing' must be a positive integer.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            close,
            volume,
            -- Calculate daily price change
            close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS price_change,
            -- Calculate volume change for RSI_Volume
            volume - LAG(volume, 1) OVER (PARTITION BY symbol ORDER BY open_time) AS volume_change
        FROM ",
    table_to_query,
    "
    ),
    rsi_data AS (
        SELECT
            symbol,
            open_time,
            close,
            volume,
            price_change,
            volume_change,
            -- Calculate average gain and loss for RSI_Volume
            CASE WHEN volume_change > 0 THEN volume_change ELSE 0 END AS volume_gain,
            CASE WHEN volume_change < 0 THEN ABS(volume_change) ELSE 0 END AS volume_loss,
            AVG(CASE WHEN volume_change > 0 THEN volume_change ELSE 0 END) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    rsi_period - 1,
    " PRECEDING AND CURRENT ROW) AS avg_gain,
            AVG(CASE WHEN volume_change < 0 THEN ABS(volume_change) ELSE 0 END) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    rsi_period - 1,
    " PRECEDING AND CURRENT ROW) AS avg_loss
        FROM
            price_data
    ),
    force_index_data AS (
        SELECT
            symbol,
            open_time,
            close,
            volume,
            price_change,
            -- Calculate Force Index
            price_change * volume AS force_index_raw,
            -- Smooth Force Index using Exponential Moving Average (EMA)
            AVG(price_change * volume) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    force_index_smoothing - 1,
    " PRECEDING AND CURRENT ROW) AS force_index_smoothed
        FROM
            price_data
    )
    SELECT
        r.symbol,
        r.open_time,
        -- Calculate RSI_Volume
        100 - (100 / (1 + NULLIF(r.avg_gain / NULLIF(r.avg_loss, 0), 0))) AS rsi_volume,
        -- Include Force Index
        f.force_index_smoothed AS force_index
    FROM
        rsi_data r
    JOIN
        force_index_data f
    ON
        r.symbol = f.symbol AND r.open_time = f.open_time
    WHERE
        r.avg_loss IS NOT NULL  -- Exclude rows where RSI cannot be calculated
    ORDER BY
        r.symbol,
        r.open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get different volume averages indicators
get_vol_averages <- function(
  db_con,
  timeframe = "1d",
  periods = c(20, 50, 150, 200)
) {
  # Ensure that the 'volume_sma_period' is valid
  if (!is.numeric(periods) || length(periods) == 0 || any(periods <= 0)) {
    stop("'periods' must be a positive integer.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
          SELECT
              symbol,
              open_time,
              close,
              volume,
              -- Calculate On-Balance Volume (OBV)
              CASE WHEN close > LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) THEN volume
                       WHEN close < LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time) THEN -volume
                       ELSE 0 END AS obv_temp,
              -- Calculate Volume-Weighted Average Price (VWAP)
              SUM(close * volume) OVER (PARTITION BY symbol ORDER BY open_time) / SUM(volume) OVER (PARTITION BY symbol ORDER BY open_time) AS vwap
          FROM ",
    table_to_query,
    "
      )
      SELECT
          symbol,
          open_time,
          vwap,
          SUM(obv_temp) OVER (PARTITION BY symbol ORDER BY open_time) AS obv"
  )

  # Loop over each period and add the corresponding AVG window function to the query
  for (period in periods) {
    # Dynamically generate the SQL for each period
    select_query <- paste0(
      select_query,
      ", AVG(volume) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
      (period - 1),
      " PRECEDING AND CURRENT ROW) AS vol_sma_",
      period,
      ", volume / NULLIF(AVG(volume) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
      (period - 1),
      " PRECEDING AND CURRENT ROW), 0) AS volume_relative_",
      period
    )
  }

  # Add the FROM clause
  select_query <- paste0(select_query, " FROM price_data")

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get the Keltner Bands for a given period
get_keltner_bands <- function(
  db_con,
  timeframe = "1d",
  period = 20,
  multiplier = 2
) {
  # Ensure that the 'period' and 'multiplier' are valid
  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a positive integer.")
  }
  if (!is.numeric(multiplier) || length(multiplier) != 1 || multiplier <= 0) {
    stop("'multiplier' must be a positive number.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            close,
            high,
            low,
            -- Calculate true range
            GREATEST(
                high - low,
                ABS(high - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)),
                ABS(low - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time))
            ) AS true_range
        FROM ",
    table_to_query,
    "
    ),
    keltner_data AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate EMA of close (middle band)
            AVG(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS middle_band,
            -- Calculate ATR
            AVG(true_range) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS atr
        FROM
            price_data
    )
    SELECT
        symbol,
        open_time,
        middle_band AS keltner_middle,
        -- Calculate upper band (middle_band + multiplier * atr)
        middle_band + (",
    multiplier,
    " * atr) AS keltner_upper,
        -- Calculate lower band (middle_band - multiplier * atr)
        middle_band - (",
    multiplier,
    " * atr) AS keltner_lower
    FROM
        keltner_data
    ORDER BY
        symbol,
        open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# get the ADX indicator
get_adx <- function(db_con, timeframe = "1d", period = 14) {
  # Ensure that the 'period' is valid
  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a positive integer.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            high,
            low,
            close,
            -- Calculate true range
            GREATEST(
                high - low,
                ABS(high - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time)),
                ABS(low - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY open_time))
            ) AS true_range,
            -- Calculate +DM and -DM
            CASE WHEN (high - LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time)) > 
                      (LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) - low)
                 THEN GREATEST(high - LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time), 0)
                 ELSE 0 END AS plus_dm,
            CASE WHEN (LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) - low) > 
                      (high - LAG(high, 1) OVER (PARTITION BY symbol ORDER BY open_time))
                 THEN GREATEST(LAG(low, 1) OVER (PARTITION BY symbol ORDER BY open_time) - low, 0)
                 ELSE 0 END AS minus_dm
        FROM ",
    table_to_query,
    "
    ),
    smoothed_data AS (
        SELECT
            symbol,
            open_time,
            true_range,
            plus_dm,
            minus_dm,
            -- Calculate smoothed true range, +DM, and -DM
            AVG(true_range) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS smoothed_tr,
            AVG(plus_dm) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS smoothed_plus_dm,
            AVG(minus_dm) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS smoothed_minus_dm
        FROM
            price_data
    ),
    di_data AS (
        SELECT
            symbol,
            open_time,
            -- Calculate +DI and -DI
            100 * (smoothed_plus_dm / smoothed_tr) AS plus_di,
            100 * (smoothed_minus_dm / smoothed_tr) AS minus_di
        FROM
            smoothed_data
    ),
    adx_data AS (
        SELECT
            symbol,
            open_time,
            plus_di,
            minus_di,
            -- Calculate DX
            100 * (ABS(plus_di - minus_di) / (plus_di + minus_di)) AS dx,
            -- Calculate ADX (smoothed DX)
            AVG(100 * (ABS(plus_di - minus_di) / (plus_di + minus_di))) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS adx
        FROM
            di_data
    )
    SELECT
        symbol,
        open_time,
        plus_di,
        minus_di,
        adx
    FROM
        adx_data
    ORDER BY
        symbol,
        open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# Get the CCI
get_cci <- function(db_con, timeframe = "1d", period = 20) {
  # Ensure that the 'period' is valid
  if (!is.numeric(period) || length(period) != 1 || period <= 0) {
    stop("'period' must be a positive integer.")
  }

  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            high,
            low,
            close,
            -- Calculate typical price
            (high + low + close) / 3 AS typical_price
        FROM ",
    table_to_query,
    "
    ),
    cci_data AS (
        SELECT
            symbol,
            open_time,
            close,
            typical_price,
            -- Calculate moving average of typical price
            AVG(typical_price) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW) AS ma_typical_price,
            -- Calculate mean deviation of typical price
            ABS(typical_price - AVG(typical_price) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS deviation
        FROM
            price_data
    )
    SELECT
        symbol,
        open_time,
        
        -- Calculate CCI
        (typical_price - ma_typical_price) / (0.015 * AVG(deviation) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (period - 1),
    " PRECEDING AND CURRENT ROW)) AS cci
    FROM
        cci_data
    ORDER BY
        symbol,
        open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# get ichimoku cloud indicator
get_ichimoku_cloud <- function(
  db_con,
  timeframe = "1d",
  tenkan_period = 9,
  kijun_period = 26,
  senkou_b_period = 52,
  chikou_shift = 26
) {
  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

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

  # Initialize the SELECT part of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            high,
            low,
            close
        FROM ",
    table_to_query,
    "
    ),
    ichimoku_data AS (
        SELECT
            symbol,
            open_time,
            close,
            -- Calculate Tenkan-sen (Conversion Line)
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (tenkan_period - 1),
    " PRECEDING AND CURRENT ROW) + 
            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (tenkan_period - 1),
    " PRECEDING AND CURRENT ROW)) / 2 AS tenkan_sen,
            -- Calculate Kijun-sen (Base Line)
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (kijun_period - 1),
    " PRECEDING AND CURRENT ROW) + 
            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (kijun_period - 1),
    " PRECEDING AND CURRENT ROW)) / 2 AS kijun_sen,
            -- Calculate Senkou Span A (Leading Span A)
            ((MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (tenkan_period - 1),
    " PRECEDING AND CURRENT ROW) + 
             MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (tenkan_period - 1),
    " PRECEDING AND CURRENT ROW)) / 2 +
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (kijun_period - 1),
    " PRECEDING AND CURRENT ROW) + 
             MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (kijun_period - 1),
    " PRECEDING AND CURRENT ROW)) / 2) / 2 AS senkou_span_a,
            -- Calculate Senkou Span B (Leading Span B)
            (MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (senkou_b_period - 1),
    " PRECEDING AND CURRENT ROW) + 
            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN ",
    (senkou_b_period - 1),
    " PRECEDING AND CURRENT ROW)) / 2 AS senkou_span_b,
            -- Calculate Chikou Span (Lagging Span)
            LAG(close, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS chikou_span,
            -- Add row number for filtering
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time) AS row_num
        FROM
            price_data
    )
    SELECT
        symbol,
        open_time,
        tenkan_sen,
        kijun_sen,
        senkou_span_a,
        senkou_span_b,
        chikou_span
        -- Shift Senkou Span A and B by the Chikou shift period
        -- LEAD(senkou_span_a, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS senkou_span_a_shifted,
        -- LEAD(senkou_span_b, ",
    chikou_shift,
    ") OVER (PARTITION BY symbol ORDER BY open_time) AS senkou_span_b_shifted
    FROM
        ichimoku_data
    WHERE row_num > ",
    senkou_b_period,
    "
    ORDER BY
        symbol,
        open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# get returns / price moves / results
get_price_moves <- function(
  db_con,
  timeframe = "1d",
  periods = c(1, 5, 10)
) {
  # Ensure that the 'timeframe' is valid
  if (!timeframe %in% c("1d", "1h")) {
    stop("Invalid timeframe. Supported values are '1d', '1h'.")
  }

  # Ensure that the 'periods' are valid
  if (!is.numeric(periods) || any(periods <= 0)) {
    stop("'periods' must be a vector of positive integers.")
  }

  # Select the correct table based on the 'timeframe'
  table_to_query <- switch(
    timeframe,
    "1d" = "daily_prices",
    "1h" = "hourly_prices"
  )

  # Initialize the WITH clause of the query
  select_query <- paste0(
    "WITH price_data AS (
        SELECT
            symbol,
            open_time,
            close"
  )

  # Add window function columns for each period
  for (period in periods) {
    select_query <- paste0(
      select_query,
      ",\n            MAX(high) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
      period,
      " FOLLOWING) AS max_high_",
      period,
      ",\n            MIN(low) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
      period,
      " FOLLOWING) AS min_low_",
      period,
      ",\n            LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN 1 FOLLOWING AND ",
      period,
      " FOLLOWING) AS future_close_",
      period
    )
  }

  # Complete the WITH clause
  select_query <- paste0(
    select_query,
    "\n        FROM ",
    table_to_query,
    "
    )
    SELECT
        symbol,
        open_time"
  )

  # Add return calculations for each period
  for (period in periods) {
    select_query <- paste0(
      select_query,
      ",\n        (max_high_",
      period,
      " - close) / close * 100 AS check_max_rise_",
      period,
      ",\n        (min_low_",
      period,
      " - close) / close * 100 AS check_max_drop_",
      period,
      ",\n        (future_close_",
      period,
      " - close) / close * 100 AS check_net_change_",
      period
    )
  }

  # Complete the query
  select_query <- paste0(
    select_query,
    "\n    FROM price_data"
  )

  # Add WHERE clause to exclude rows with NULL values for any period
  where_clauses <- sapply(periods, function(p) {
    c(
      paste0("max_high_", p, " IS NOT NULL"),
      paste0("min_low_", p, " IS NOT NULL"),
      paste0("future_close_", p, " IS NOT NULL")
    )
  })

  select_query <- paste0(
    select_query,
    "\n    WHERE ",
    paste(where_clauses, collapse = " AND "),
    "\n    ORDER BY symbol, open_time;"
  )

  # Execute the query
  result <- dbGetQuery(db_con, select_query)

  # Return the result
  return(result)
}

# # Get the earnings calendar
get_earnings_calendar <- function(db_con) {
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

# Function to get benchmarks indicators
# This function loads and processes external market indexes data to use as benchmarks
get_external_indexes <- function(
  db_con,
  start_date = NULL,
  end_date = NULL,
  symbols = NULL
) {
  # Create a db_con to the database
  db_con

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
    "SELECT * FROM daily_prices WHERE symbol IN (",
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
    select(symbol, open_time, close) %>%
    pivot_wider(
      names_from = symbol,
      values_from = close
    )

  # Return the processed data
  return(external_indexes_data)
}
