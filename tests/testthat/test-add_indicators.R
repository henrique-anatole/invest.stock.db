# Create a temporary duckdb database with sample data for testing
temp_db <- tempfile(fileext = ".duckdb")
temp_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = temp_db, read_only = FALSE)


# Add each of the tables from the sample_stock_prices dataset to the temporary database for testing
for (table_name in names(sample_stock_prices)) {
  DBI::dbWriteTable(
    temp_con,
    table_name,
    sample_stock_prices[[table_name]],
    overwrite = TRUE
  )
}

# Add each of the tables from the sample_fundamentals dataset to the temporary database for testing
for (table_name in names(sample_fundamentals)) {
  DBI::dbWriteTable(
    temp_con,
    table_name,
    sample_fundamentals[[table_name]],
    overwrite = TRUE
  )
}
benchmarks <- invest.data::create_benchmarks()
DBI::dbWriteTable(
  temp_con,
  "benchmark_symbols",
  benchmarks,
  overwrite = TRUE
)

#tables in the database
DBI::dbListTables(temp_con)

# read the daily_prices table to use as input for the add_indicators function
prepared_data <- DBI::dbReadTable(temp_con, "daily_prices")

#create a fake daily price series for ^GSPC and insert into the database
gspc_data <- data.frame(
  symbol = "^GSPC",
  open_time = seq(
    min(prepared_data$open_time),
    max(prepared_data$open_time),
    by = "days"
  ),
  open = runif(
    length(seq(
      min(prepared_data$open_time),
      max(prepared_data$open_time),
      by = "days"
    )),
    3000,
    4000
  ),
  high = runif(
    length(seq(
      min(prepared_data$open_time),
      max(prepared_data$open_time),
      by = "days"
    )),
    3000,
    4000
  ),
  low = runif(
    length(seq(
      min(prepared_data$open_time),
      max(prepared_data$open_time),
      by = "days"
    )),
    3000,
    4000
  ),
  close = runif(
    length(seq(
      min(prepared_data$open_time),
      max(prepared_data$open_time),
      by = "days"
    )),
    3000,
    4000
  ),
  volume = NA
)
DBI::dbWriteTable(
  temp_con,
  "daily_prices",
  gspc_data,
  append = TRUE
)
# read the daily_prices table again to use as input for the add_indicators function, now with the ^GSPC data included
prepared_data <- DBI::dbReadTable(temp_con, "daily_prices")

test_that("add_indicators - first set", {
  # test all indicators together
  indicators <- c(
    "smas",
    "ema",
    "bollinger",
    "macd",
    "volatility",
    "gaps",
    "min_max",
    "pivots",
    "rsi",
    "stochastic_k",
    "momentum",
    "roc",
    "ker",
    "rsi_volume",
    "force_index",
    "vol_sma",
    "obv",
    "volume_relative",
    "vwap",
    "keltner_bands",
    "adx",
    "cci",
    "ichimoku_cloud",
    "external_indexes",
    "eps_data",
    "calendar"
  )

  result <- add_indicators(
    prepared_data = prepared_data,
    indicators = indicators,
    db_con = temp_con
  )
  names(result)
  # any column named with .x or .y?
  expect_false(any(grepl("\\.x|\\.y", names(result))))
  # Check if the result is a data frame
  expect_true(is.data.frame(result))
  # Check if the expected columns are present in the result
  expected_price_columns <- c(
    "symbol",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume"
  )
  expected_indicator_columns <- c(
    "sma_20",
    "sma_50",
    "ema_20",
    "ema_50",
    "sma_20bb",
    "stddev_20bb",
    "upper_band_20",
    "lower_band_20",
    "distance_upper_band_20",
    "distance_lower_band_20",
    "distance_high_upper_band_20",
    "distance_low_lower_band_20",
    "macd",
    "macd_signal",
    "macd_histogram",
    "intraday_volatility",
    "volatility_20",
    "volatility_50",
    "gap_up",
    "gap_down",
    "significant_gap",
    "upper_donchian_120",
    "lower_donchian_120",
    "pivot_line",
    "r1",
    "s1",
    "r2",
    "s2",
    "rsi",
    "stochastic_k",
    "momentum",
    "roc",
    "ker",
    "rsi_volume",
    "force_index",
    "vol_sma_20",
    "vol_sma_50",
    "vol_rel_150",
    "vol_rel_200",
    "anchored_obv",
    "anchored_vwap",
    "keltner_upper",
    "keltner_middle",
    "keltner_lower",
    "plus_di",
    "minus_di",
    "adx",
    "cci",
    "tenkan_sen",
    "kijun_sen",
    "senkou_span_a",
    "senkou_span_b",
    "chikou_span",
    "before_open",
    "after_close",
    "eps_estimated",
    "consecutive_growth",
    "quarter",
    "weekday",
    "day_of_month",
    "noise",
    "^gspc"
  )
  expect_true(all(expected_price_columns %in% tolower(names(result))))
  expect_true(all(expected_indicator_columns %in% tolower(names(result))))

  # Check the exclusive indicators
  indicators <- c(
    "rsi",
    "momentum",
    "rsi_volume",
    "vol_sma"
  )

  result <- add_indicators(
    prepared_data = prepared_data,
    indicators = indicators,
    db_con = temp_con
  )
  # No column stochastic_k should be present
  expect_false(any(grepl("stochastic_k", names(result))))
  expect_false(any(grepl("roc", names(result))))
  expect_false(any(grepl("force_index", names(result))))
  expect_false(any(grepl("vol_sma", names(result))))
  expect_false(any(grepl("vol_rel", names(result))))

  indicators <- c(
    "stochastic_k",
    "roc",
    "force_index",
    "volume_relative"
  )

  result <- add_indicators(
    prepared_data = prepared_data,
    indicators = indicators,
    db_con = temp_con
  )
  # No column rsi should be present
  expect_false(any(grepl("rsi", names(result))))

  indicators <- c(
    "ker",
    "obv"
  )

  indicators <- c(
    "vwap"
  )
})
