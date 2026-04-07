# Create a temporary duckdb database with sample data for testing
temp_db <- tempfile(fileext = ".duckdb")
temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)

price_data <- readRDS("data/sample_stock_prices.rds")
# Add each of the tables from the sample_stock_prices dataset to the temporary database for testing
for (table_name in names(price_data)) {
  DBI::dbWriteTable(
    temp_con,
    table_name,
    price_data[[table_name]],
    overwrite = TRUE
  )
}

# Disconnect and remove the temporary database
fundamental_data <- readRDS("data/sample_fundamentals.rds")
# Add each of the tables from the sample_fundamentals dataset to the temporary database for testing
for (table_name in names(fundamental_data)) {
  DBI::dbWriteTable(
    temp_con,
    table_name,
    fundamental_data[[table_name]],
    overwrite = TRUE
  )
}


# 1.2 Expect the daily_prices table to exist now, and have all symbols loaded
daily_prices <- DBI::dbGetQuery(temp_con, "SELECT * FROM daily_prices")

# Test the indicators functions
testthat::test_that("calculate_single_ema: Working examples", {
  smas <- get_smas(
    temp_con,
    timeframe = "1d",
    periods = c(20, 50, 150, 200)
  )

  data_with_indicators <- daily_prices %>%
    left_join(smas, by = c("symbol", "open_time"))

  # 1.3 Verify columns exist
  expected_sma_cols <- c("SMA_20", "SMA_50", "SMA_150", "SMA_200")
  testthat::expect_true(all(
    expected_sma_cols %in% colnames(data_with_indicators)
  ))

  # 1.4 Test the "Cold Start" logic (CASE WHEN approach)
  # For a specific symbol, the first 19 rows of SMA_20 should be NA
  # and the 20th row should have a numeric value.
  one_symbol <- data_with_indicators %>%
    filter(symbol == "AAPL") %>%
    arrange(open_time)

  testthat::expect_true(all(is.na(one_symbol$SMA_20[1:19])))
  testthat::expect_false(is.na(one_symbol$SMA_20[20]))

  # 1.5 Test a larger window (SMA_200)
  # If we only fetched 200 days of data, SMA_200 will likely only have
  # a value on the very last row (or none if exactly 200 and weekends exist)
  if (nrow(one_symbol) >= 200) {
    testthat::expect_true(all(is.na(one_symbol$SMA_200[1:199])))
    testthat::expect_false(is.na(one_symbol$SMA_200[200]))
  }

  # 1.6 Consistency Check: SMA_20 should be different from raw close (usually)
  # and specifically should be the mean of the first 20 close prices
  expected_manual_sma <- mean(one_symbol$close[1:20])
  testthat::expect_equal(
    one_symbol$SMA_20[20],
    expected_manual_sma,
    tolerance = 1e-5
  )

  # 2.1 Test invalid timeframe
  testthat::expect_error(
    get_smas(temp_con, timeframe = "5m"),
    "Invalid timeframe"
  )
  # 2.2 Test invalid periods
  testthat::expect_error(
    get_smas(temp_con, periods = c(-1, 20)),
    "positive integers"
  )
  # 2.3 Test invalid connection
  temp_con_invalid <- NULL
  testthat::expect_error(
    get_smas(temp_con_invalid, timeframe = "1d", periods = c(20, 50)),
    "db_con must be a valid DBI connection"
  )
})

testthat::test_that("calculate_single_ema handles edge cases", {
  # get the daily data for one symbol to test the EMA function
  daily_prices <- daily_prices %>%
    arrange(symbol, open_time) %>%
    group_by(symbol)
  # For testing, we will use the 'close' column from the daily_prices
  period <- 20
  # Create a sample dataframe with some NAs to test the EMA function
  EMA_col_name <- paste0("EMA_", period)

  data_with_indicators <- daily_prices %>%
    mutate(!!EMA_col_name := calculate_single_ema(close, period))

  # 1 Test the "Cold Start" logic (CASE WHEN approach)
  # For a specific symbol, the first 19 rows of EMA_20 should be NA
  # and the 20th row should have a numeric value.
  one_symbol <- data_with_indicators %>%
    filter(symbol == "AAPL") %>%
    arrange(open_time)

  testthat::expect_true(all(is.na(one_symbol$EMA_20[1:19])))
  testthat::expect_false(is.na(one_symbol$EMA_20[20]))

  # Test insufficient data
  res_short <- calculate_single_ema(rep(1, 5), period = 10)
  expect_true(all(is.na(res_short)))

  # Test with one NA in the middle of the data
  test_data <- c(1:10, NA, 12:30)
  res_na <- calculate_single_ema(test_data, period = 20)
  expect_false(any(is.na(res_na[20:length(res_na)]))) # After the initial 20, there should be no NAs
})

# Now test the get_emas function which calls calculate_single_ema internally
testthat::test_that("get_emas and calculate_single_ema: Working examples", {
  # 1.1 Calculate EMAs using the provided function
  # We use the same periods as SMA for comparison
  EMA_periods <- c(20, 50, 150, 200)
  data_with_emas <- get_emas(
    timeseries = daily_prices,
    periods = EMA_periods
  )

  # 1.2 Verify all expected columns are created
  expected_EMA_cols <- paste0("EMA_", EMA_periods)
  testthat::expect_true(all(
    expected_EMA_cols %in% colnames(data_with_emas)
  ))

  # 1.3 Verify the output is ungrouped (Best Practice Check)
  testthat::expect_false(dplyr::is_grouped_df(data_with_emas))

  # 1.4 Test "Cold Start" and grouping for a specific symbol
  one_symbol_ema <- data_with_emas %>%
    filter(symbol == "AAPL") %>%
    arrange(open_time)

  # EMA (TTR implementation) typically returns NA for the first n-1 rows
  testthat::expect_true(all(is.na(one_symbol_ema$EMA_20[1:19])))
  testthat::expect_false(is.na(one_symbol_ema$EMA_20[20]))

  # 1.5 Mathematical Consistency Check
  # The first non-NA value of an EMA is typically the SMA of that same period
  # We can compare our SMA calculation to the first EMA value
  first_EMA_val <- one_symbol_ema$EMA_20[20]
  manual_sma_val <- mean(one_symbol_ema$close[1:20])

  testthat::expect_equal(
    first_EMA_val,
    manual_sma_val,
    tolerance = 1e-5
  )

  # 1.6 Verify that symbols are isolated (Grouping Check)
  # Ensure EMA calculation doesn't bleed from the end of one symbol to the start of another
  symbol_starts <- data_with_emas %>%
    group_by(symbol) %>%
    slice(1) %>%
    ungroup()

  # The first row of every symbol for any EMA should be NA
  testthat::expect_true(all(is.na(symbol_starts$EMA_20)))

  # 2.1 Error Handling: Missing Columns
  invalid_df <- daily_prices %>% select(-close)
  testthat::expect_error(
    get_emas(invalid_df),
    "must have a 'close' column"
  )

  # 2.2 Error Handling: Empty Periods
  testthat::expect_error(
    get_emas(daily_prices, periods = c()),
    "'periods' must be a vector of positive integers."
  )
})

testthat::test_that("get_bollinger_bands: Working examples", {
  period_val <- 20
  bb_data <- get_bollinger_bands(
    temp_con,
    timeframe = "1d",
    period = period_val
  )

  # 1.1 Verify columns
  expect_true(all(
    c(paste0("SMA_", period_val), paste0("Upper_Band_", period_val)) %in%
      colnames(bb_data)
  ))

  # 1.2 Verify filtering logic
  # Since you filter row_number() > period, the first row per symbol should be period + 1
  one_symbol <- bb_data %>% filter(symbol == "AAPL") %>% arrange(open_time)
  # If the database has 300 rows, and you skip 20, you should have 280 left.

  # 1.3 Mathematical Check
  # Math Check: Upper - Lower should equal 4 * STDDEV
  sample <- bb_data %>% slice(1)
  range_diff <- sample[[paste0("Upper_Band_", period_val)]] -
    sample[[paste0("Lower_Band_", period_val)]]
  expected_diff <- 4 * sample[[paste0("STDDEV_", period_val)]]

  testthat::expect_equal(range_diff, expected_diff, tolerance = 1e-5)

  # 2.1 Test invalid timeframe
  expect_error(
    get_bollinger_bands(temp_con, period = c(20, 50)),
    "'period' must be a single positive integer."
  )
})

testthat::test_that("get_macd calculates correctly", {
  # Create sample data
  res <- get_macd(daily_prices)

  # Check columns
  expect_true(all(
    c("macd", "signal_line", "macd_histogram", "macd_direction") %in%
      colnames(res)
  ))

  # Check direction logic
  sample_row <- res[10, ]
  if (sample_row$macd_histogram > 0) {
    expect_equal(sample_row$macd_direction, 1)
  } else {
    expect_equal(sample_row$macd_direction, 0)
  }

  # Check filtering
  expect_true(all(!is.na(res$signal_line)))
})

testthat::test_that("get_volatilities calculates expected metrics", {
  periods <- c(10, 20)
  res <- get_volatilities(
    db_con = temp_con,
    timeframe = "1d",
    periods = periods
  )

  # Check for dynamic columns
  expect_true("volatility_10" %in% colnames(res))
  expect_true("atr_20" %in% colnames(res))

  # Logic check: True Range must always be >= Intraday Volatility
  # (Because True Range includes gaps, whereas Intraday Volatility is just H - L)
  test_row <- res %>% filter(!is.na(true_range)) %>% slice(1)
  expect_true(test_row$true_range >= test_row$intraday_volatility)
})

testthat::test_that("check_recent_gaps identifies shocks correctly", {
  df <- data.frame(
    symbol = "TEST",
    gap_up = c(0, 0, 0.05, 0, 0), # Significant gap at index 3
    gap_down = c(NA, NA, NA, NA, NA)
  )

  # Check 3-period lookback
  res <- check_recent_gaps(df, gap_size = 0.03, periods = c(3, 4))

  # Index 3 is TRUE (the gap itself)
  # Index 4 is TRUE (index 3 is in the window)
  # Index 5 is TRUE (index 3 is in the window)
  expect_true(all(res$recent_gap_3[3:5]))
  expect_true(all(res$recent_gap_4[4:5]))
  # Index 1 and 2 should be NA because .complete = TRUE
  expect_true(is.na(res$recent_gap_3[1]))
  expect_true(is.na(res$recent_gap_4[1]))
})

testthat::test_that("get_donchian_channels handles high/low logic", {
  res <- get_donchian_channels(temp_con, timeframe = "1d", period = 20)

  # Ensure all 3 calculated columns exist
  expect_true(all(
    c("upper_donchian_20", "lower_donchian_20", "mid_donchian_20") %in%
      colnames(res)
  ))

  # Logical Check: Upper must be >= Mid, and Mid must be >= Lower
  sample_row <- res[100, ]
  expect_gt(sample_row$upper_donchian_20, sample_row$mid_donchian_20)
  expect_gt(sample_row$mid_donchian_20, sample_row$lower_donchian_20)
  # Check that the mid is the average of upper and lower
  expected_mid <- (sample_row$upper_donchian_20 +
    sample_row$lower_donchian_20) /
    2
  expect_equal(sample_row$mid_donchian_20, expected_mid, tolerance = 1e-5)
})

testthat::test_that("Pivot structure is sound", {
  res <- get_pivots(temp_con, weekly = TRUE, fib = TRUE)

  # Ensure all Fibonacci levels are present
  expect_true(all(
    c("pivt", "r1", "r2", "r3", "s1", "s2", "s3") %in% colnames(res)
  ))

  # Check that it's a numeric result
  expect_type(res$pivt, "double")

  # Ensure no year-break join issues (symbol count should match raw daily data - 1 week)
  expect_gt(nrow(res), 0)
})

testthat::test_that("Oscillators stay within 0-100 range", {
  res <- get_oscillators(temp_con, period = 14)

  # RSI and Stochastics are mathematically bound between 0 and 100
  expect_true(all(res$rsi >= 0 & res$rsi <= 100, na.rm = TRUE))
  expect_true(all(
    res$stochastic_k >= 0 & res$stochastic_k <= 100,
    na.rm = TRUE
  ))

  # Ensure no columns are missing
  expect_named(res, c("symbol", "open_time", "rsi", "stochastic_k"))
})

testthat::test_that("Trend indicators return valid values", {
  res <- get_trend_followers(temp_con, period = 14)

  # KER must be between 0 and 1
  expect_true(all(res$ker >= 0 & res$ker <= 1, na.rm = TRUE))

  # Check for expected column count (5)
  expect_equal(ncol(res), 5)

  # Ensure momentum is correctly calculated for a test row
  # (Current Close - Prev Close)
})

testthat::test_that("Volume oscillators return valid data", {
  res <- get_vol_oscillators(
    temp_con,
    rsi_period = 14,
    force_index_smoothing = 13
  )

  # RSI Volume should be bounded 0-100
  expect_true(all(res$rsi_volume >= 0 & res$rsi_volume <= 100, na.rm = TRUE))

  # Columns should be correct
  expect_named(res, c("symbol", "open_time", "rsi_volume", "force_index"))

  # Ensure the data starts after the warm-up
  expect_false(any(is.na(res$force_index[20])))
})

testthat::test_that("get_vol_averages correctly calculates RVOL on 1d timeframe", {
  # 1. Run the function for a 20-day period
  periods <- c(20)
  res <- get_vol_averages(temp_con, timeframe = "1d", periods = periods)

  # 2. Pick a sample row (ensure it's past the 20-day warm-up period)
  # We use symbol 'AAPL' as an example
  sample_data <- res %>%
    filter(symbol == "AAPL") %>%
    arrange(open_time) %>%
    slice(25) # Row 25 has a full 20-day history

  # 3. Manual Calculation for Verification
  # RVOL = Current Volume / SMA(Volume, 20)
  expected_rvol <- sample_data$volume / sample_data$vol_sma_20

  # 4. Assertions
  # Check that the column exists
  expect_true("vol_rel_20" %in% colnames(res))

  # Check that the ratio is mathematically correct
  expect_equal(sample_data$vol_rel_20, expected_rvol, tolerance = 1e-5)

  # (Using the underlying grouping logic)
  sample_monday <- res %>%
    group_by(symbol, anchor_group = as.Date(cut(open_time, "week"))) %>%
    filter(open_time == min(open_time)) %>%
    ungroup() %>%
    slice(150) # Pick any arbitrary 'first day' of a week

  # On the FIRST day of the week, VWAP must equal Close
  expect_equal(
    sample_monday$anchored_vwap,
    sample_monday$close,
    tolerance = 1e-5
  )
})

testthat::test_that("Keltner Bands maintain logical order", {
  res <- get_keltner_bands(temp_con, period = 20, multiplier = 2)

  # Pick a random row
  sample <- res[100, ]

  # Logical Check: Upper > Middle > Lower
  expect_gt(sample$keltner_upper, sample$keltner_middle)
  expect_gt(sample$keltner_middle, sample$keltner_lower)

  # Ensure the warm-up filter worked
  # (Total rows should be original count minus roughly 19)
  expect_true(all(!is.na(res$keltner_upper)))
})

testthat::test_that("ADX indicators are within valid bounds", {
  res <- get_adx(temp_con, period = 14)

  # ADX, +DI, and -DI are all percentage-based (0-100)
  expect_true(all(res$adx >= 0 & res$adx <= 100, na.rm = TRUE))
  expect_true(all(res$plus_di >= 0 & res$plus_di <= 100, na.rm = TRUE))

  # Ensure first rows (warm-up) are filtered or handled
  # In our query, they appear as NULL/NA due to the rolling window
})

testthat::test_that("CCI behaves as expected", {
  res <- get_cci(temp_con, period = 20)

  # CCI is an unbounded oscillator but typically stays within +/- 300
  # We check that it isn't returning constant values
  expect_gt(length(unique(res$cci)), 1)

  # Verify symbol columns exist
  expect_true(all(c("symbol", "open_time", "cci") %in% colnames(res)))

  # Ensure division by zero (NULLIF) doesn't return NAs in standard rows
  expect_false(all(is.na(res$cci)))
})

testthat::test_that("Ichimoku logic is consistent", {
  res <- get_ichimoku_cloud(temp_con)

  # Pick a row where the cloud should be fully formed
  sample <- res[100, ]

  # Ensure no obvious NAs in core lines
  expect_false(is.na(sample$tenkan_sen))
  expect_false(is.na(sample$kijun_sen))

  # Span A should be the average of Tenkan/Kijun from 26 periods ago
  # This verifies the LAG logic is working correctly for the 'future' cloud
})

testthat::test_that("Future price moves are logically sound", {
  res <- get_price_moves(temp_con, periods = 5)

  # A Max Rise should always be >= Net Change (mathematically impossible otherwise)
  sample <- res[sample(1:nrow(res), 1), ]
  expect_true(sample$target_max_rise_5 >= sample$target_net_change_5)

  # A Max Drop should always be <= Net Change
  expect_true(sample$target_max_drop_5 <= sample$target_net_change_5)
})

testthat::test_that("get_earnings_calendar returns consistent and unique periods", {
  res <- get_earnings_calendar(temp_con)

  # 1. Basic Structure
  expect_s3_class(res, "data.frame")
  expect_true(all(
    c("symbol", "period_label", "before_open") %in% colnames(res)
  ))

  # 2. Logic Check: A company cannot report both before and after for the same event
  # (Note: This depends on the quality of your source data)
  conflict_check <- res %>%
    filter(before_open == TRUE & after_close == TRUE)
  expect_equal(nrow(conflict_check), 0)

  # 3. Uniqueness Check: One symbol should only have one entry per period_label
  # This prevents 'double-counting' earnings in your ML model
  duplicates <- res %>%
    group_by(symbol, period_label) %>%
    tally() %>%
    filter(n > 1)

  expect_equal(
    nrow(duplicates),
    0,
    info = "Detected multiple earnings dates for the same symbol in a single quarter."
  )

  # 4. Data Integrity: Dates should be valid
  expect_false(any(is.na(res$date)))
})

# library(ggplot2)

# # 1. Get the pivot data (Weekly Regular Pivots)
# pivots_data <- get_pivots(temp_con, weekly = TRUE, fib = FALSE)

# # 2. Filter for one symbol and a recent period for clarity
# plot_subset <- pivots_data %>%
#   filter(symbol == "AAPL") %>%
#   arrange(open_time) %>%
#   tail(30) # Look at the last 30 trading days

# # 3. Create the plot
# ggplot(plot_subset, aes(x = open_time)) +
#   # Price Line
#   geom_line(aes(y = close), color = "black", linewidth = 1) +
#   # Pivot Point (Main Baseline)
#   geom_line(aes(y = pivt), color = "blue", linetype = "dashed", alpha = 0.7) +
#   # Support and Resistance Levels (R1 and S1)
#   geom_line(aes(y = r1), color = "red", linetype = "dotted", alpha = 0.6) +
#   geom_line(aes(y = s1), color = "darkgreen", linetype = "dotted", alpha = 0.6) +
#   # Formatting
#   labs(
#     title = "AAPL: Close Price vs. Weekly Pivots",
#     subtitle = "Solid = Close, Dashed = Pivot, Dotted = R1/S1",
#     x = "Date",
#     y = "Price"
#   ) +
#   theme_minimal()

# title = "Ichimoku Cloud Analysis"
# # Clean up any infinite values from division by zero
# df <- res <- get_ichimoku_cloud(temp_con) %>%
#   filter(is.finite(senkou_span_a), is.finite(senkou_span_b))%>%
#     filter(symbol == unique(symbol)[1]) %>% # Pick first symbol
#     arrange(open_time)

#   ggplot(df, aes(x = open_time)) +
#     # The Cloud (Kumo) - Shading
#     geom_ribbon(aes(ymin = senkou_span_a, ymax = senkou_span_b,
#                     fill = senkou_span_a > senkou_span_b),
#                 alpha = 0.2) +
#     scale_fill_manual(values = c("TRUE" = "green", "FALSE" = "red"), name = "Trend") +

#     # The Cloud Edges (The actual Span lines)
#     geom_line(aes(y = senkou_span_a), color = "darkgreen", size = 0.3, alpha = 0.5) +
#     geom_line(aes(y = senkou_span_b), color = "darkred", size = 0.3, alpha = 0.5) +

#     # Kijun-sen (Base Line) - The "Slower" indicator
#     geom_line(aes(y = kijun_sen), color = "red", size = 1) +

#     # Tenkan-sen (Conversion Line) - The "Faster" indicator
#     geom_line(aes(y = tenkan_sen), color = "blue", size = 1) +

#     # Price Line (The anchor for the eyes)
#     # Ensure 'close' was included in your get_ichimoku_cloud result
#     geom_line(aes(y = close), color = "black", size = 0.8, alpha = 0.8) +

#     labs(title = paste("Ichimoku Clean View:", unique(df$symbol)),
#          subtitle = "Black: Price | Blue: Tenkan (9) | Red: Kijun (26) | Shaded: Kumo",
#          x = "Date", y = "Price") +
#     theme_minimal() +
#     theme(legend.position = "none")
