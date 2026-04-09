#' Orchestrate Indicator Addition
#' @description Merges price data with technical indicators, fundamental data, and model predictions.
#' @param prepared_data Dataframe containing 'symbol' and 'open_time'.
#' @param indicators Character vector of requested indicator names.
#' @param model_labels Optional labels for XGBoost model fetching.
#' @param target_condition List containing success/failure thresholds.
#' @param db_con Database connection object.
#' @return A consolidated dataframe with all requested features.
#' @export
add_indicators <- function(
  prepared_data,
  indicators,
  model_labels = NULL,
  target_condition = NULL,
  db_con
) {
  # Join all data and create features for all symbols at once
  indicators <- tolower(indicators)
  # Check for the indicators that were passed and add them to the prepared data
  # smas
  if (any(grepl("smas", indicators))) {
    smas <- get_smas(
      db_con,
      timeframe = "1d",
      periods = c(20, 50, 150, 200)
    )
    prepared_data <- prepared_data %>%
      left_join(smas, by = c("symbol", "open_time"))
  }
  # emas
  if (any(grepl("ema", indicators))) {
    prepared_data <- prepared_data %>%
      get_emas(periods = c(20, 50, 150, 200))
  }
  # bolinger_bands
  if (any(grepl("bollinger", indicators))) {
    bollinger_bands <- get_bollinger_bands(
      db_con,
      timeframe = "1d",
      period = 20
    )
    prepared_data <- prepared_data %>%
      left_join(bollinger_bands, by = c("symbol", "open_time"))
  }
  # macd
  if (any(grepl("macd", indicators))) {
    prepared_data <- prepared_data %>%
      get_macd(
        fast_period = 12,
        slow_period = 26,
        signal_period = 9,
        maType = "EMA",
        percent = TRUE
      )
  }
  # volatilities
  if (any(grepl("volatility", indicators))) {
    volatilities <- get_volatilities(
      db_con,
      timeframe = "1d",
      periods = c(10, 20, 50)
    )
    prepared_data <- prepared_data %>%
      left_join(volatilities, by = c("symbol", "open_time"))
  }
  # check_recent_gaps
  if (any(grepl("gaps", indicators))) {
    # check if prepared_data has the columns gap_down and gap_up
    if (!all(c("gap_down", "gap_up") %in% colnames(prepared_data))) {
      volatilities <- get_volatilities(
        db_con,
        timeframe = "1d",
        periods = c(10, 20, 50)
      )
      prepared_data <- prepared_data %>%
        left_join(volatilities, by = c("symbol", "open_time"))
    }

    prepared_data <- prepared_data %>%
      check_recent_gaps(gap_size = 0.03, periods = c(5))
  }
  # min_max
  # grepl any mention of min_max or donchian in the indicators vector
  if (any(grepl("min_max|donchian", indicators))) {
    min_max <- get_donchian_channels(db_con, timeframe = "1d", period = 120)
    prepared_data <- prepared_data %>%
      left_join(min_max, by = c("symbol", "open_time"))
  }
  # pivots
  if (any(grepl("pivots", indicators))) {
    pivots <- get_pivots(db_con, weekly = TRUE, fib = FALSE)
    prepared_data <- prepared_data %>%
      left_join(pivots, by = c("symbol", "open_time"))
  }
  # RSI
  if (any(grepl("rsi|stochastic_k", indicators))) {
    # load Oscillators if not already loaded
    if (!exists("oscillators")) {
      oscillators <- get_oscillators(db_con, timeframe = "1d", period = 14)
    }
    prepared_data <- prepared_data %>%
      left_join(oscillators, by = c("symbol", "open_time"))
    # remove the columns not requested
    non_requested_cols <- c(
      "rsi",
      "stochastic_k"
    )[!c("rsi", "stochastic_k") %in% indicators]
    if (length(non_requested_cols) > 0) {
      prepared_data <- prepared_data %>%
        select(-all_of(non_requested_cols))
    }
  }

  # momentum
  if (any(grepl("momentum|roc|ker", indicators))) {
    # load Trend Followers if not already loaded
    if (!exists("trend_followers")) {
      trend_followers <- get_trend_followers(
        db_con,
        timeframe = "1d",
        period = 14
      )
    }
    prepared_data <- prepared_data %>%
      left_join(trend_followers, by = c("symbol", "open_time"))
    # remove the columns not requested
    non_requested_cols <- c(
      "momentum",
      "roc",
      "ker"
    )[!c("momentum", "roc", "ker") %in% indicators]
    if (length(non_requested_cols) > 0) {
      prepared_data <- prepared_data %>%
        select(-all_of(non_requested_cols))
    }
  }
  # RSI_Volume
  if (any(grepl("rsi_volume|force_index", indicators))) {
    # load Volume Oscillators if not already loaded
    if (!exists("volume_oscillators")) {
      volume_oscillators <- get_vol_oscillators(
        db_con,
        timeframe = "1d",
        rsi_period = 14,
        force_index_smoothing = 13
      )
    }
    prepared_data <- prepared_data %>%
      left_join(volume_oscillators, by = c("symbol", "open_time"))
    # remove the columns not requested
    non_requested_cols <- c(
      "rsi_volume",
      "force_index"
    )[!c("rsi_volume", "force_index") %in% indicators]
    if (length(non_requested_cols) > 0) {
      prepared_data <- prepared_data %>%
        select(-all_of(non_requested_cols))
    }
  }
  # vol_sma
  if (any(grepl("vol_sma|volume_relative|obv|vwap", indicators))) {
    vol_sma <- get_vol_averages(
      db_con,
      timeframe = "1d",
      periods = c(20, 50, 150, 200)
    )
    prepared_data <- prepared_data %>%
      left_join(vol_sma, by = c("symbol", "open_time"))
    # remove the columns not requested
    cols_per_indicator <- list(
      vol_sma = paste0("vol_sma_", c(20, 50, 150, 200)),
      volume_relative = paste0("vol_rel_", c(20, 50, 150, 200)),
      obv = "anchored_obv",
      vwap = "anchored_vwap"
    )
    non_requested_cols <- c(
      "vol_sma",
      "volume_relative",
      "obv",
      "vwap"
    )[!c("vol_sma", "volume_relative", "obv", "vwap") %in% indicators]
    if (length(non_requested_cols) > 0) {
      cols_to_remove <- unlist(cols_per_indicator[non_requested_cols])
      prepared_data <- prepared_data %>%
        select(-all_of(cols_to_remove))
    }
  }
  # keltner_bands
  if (any(grepl("keltner_bands", indicators))) {
    keltner_bands <- get_keltner_bands(
      db_con,
      timeframe = "1d",
      period = 20,
      multiplier = 2
    )
    prepared_data <- prepared_data %>%
      left_join(keltner_bands, by = c("symbol", "open_time"))
  }
  # adx
  if (any(grepl("adx", indicators))) {
    adx <- get_adx(db_con, timeframe = "1d", period = 14)
    prepared_data <- prepared_data %>%
      left_join(adx, by = c("symbol", "open_time"))
  }
  # cci
  if (any(grepl("cci", indicators))) {
    cci <- get_cci(db_con, timeframe = "1d", period = 20)
    prepared_data <- prepared_data %>%
      left_join(cci, by = c("symbol", "open_time"))
  }
  # ichimoku_cloud
  if (any(grepl("ichimoku_cloud", indicators))) {
    ichimoku_cloud <- get_ichimoku_cloud(
      db_con,
      timeframe = "1d",
      tenkan_period = 9,
      kijun_period = 26,
      senkou_b_period = 52,
      chikou_shift = 26
    )
    prepared_data <- prepared_data %>%
      left_join(ichimoku_cloud, by = c("symbol", "open_time"))
  }
  # external_indexes
  if (any(grepl("external_indexes", indicators))) {
    external_indexes <- get_external_indexes(db_con)
    prepared_data <- prepared_data %>%
      left_join(external_indexes, by = c("open_time"))
  }
  # earnings_calendar
  if (any(grepl("eps_data", indicators))) {
    prepared_data <- get_eps_data(
      db_con,
      prepared_data
      # , fake_Q3_2019 = FALSE
    )
  }
  # xgb_model
  if (any(grepl("xgb_model", indicators)) & !is.null(model_labels)) {
    xgb_model <- get_xgb_indicator(model_labels = model_labels)

    # label = unique(xgb_model$model_label)[1]

    transform_model_indicators <- function(label, xgb_model) {
      l = gsub("xgboost_", "", label)
      indicators <- xgb_model %>%
        filter(model_label == label) %>%
        select(-model_label) %>%
        rename_with(~ gsub("xgb", l, .), starts_with("xgb_"))
    }

    xgboost_indicators <- lapply(
      unique(xgb_model$model_label),
      transform_model_indicators,
      xgb_model = xgb_model
    )

    # left_join each dataframe in xgboost indicators to the prepared data, without a for loop
    for (i in seq_along(xgboost_indicators)) {
      # left join the xgboost indicators to the prepared data
      prepared_data <- prepared_data %>%
        left_join(xgboost_indicators[[i]], by = c("symbol", "open_time"))
    }
  }
  # Check if any variable in target_condition is NA. If all are valid, we run this target check
  if (
    !is.null(target_condition) &
      !any(sapply(target_condition, function(x) any(is.na(x))))
  ) {
    high_threshold = target_condition$success_return * 100
    low_threshold = target_condition$failure_return * 100
    max_period = target_condition$periods

    prepared_data <- check_high_before_low(
      prepared_data,
      high_threshold = high_threshold,
      low_threshold = low_threshold,
      max_period = max_period
    )
  }
  # week and month days
  if (any(grepl("calendar", indicators))) {
    prepared_data <- prepared_data %>%
      mutate(
        weekday = lubridate::wday(open_time),
        day_of_month = lubridate::day(open_time)
      )
  }
  # Load price moves
  price_moves <- get_price_moves(db_con)

  prepared_data <- prepared_data %>%
    left_join(price_moves, by = c("symbol", "open_time")) %>%
    # Process all symbols together
    mutate(
      across(where(is.logical), ~ ifelse(is.na(.), FALSE, .))
    )

  # add a column with noise
  prepared_data$noise <- rnorm(nrow(prepared_data), mean = 0, sd = 0.01)

  # I believe adujusted is redundant and should also be removed by default
  prepared_data <- prepared_data %>%
    select(-c("adjusted"))

  return(prepared_data)
}
