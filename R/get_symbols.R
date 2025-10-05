#' Function to get all stock symbols from the DuckDB database
#'
#' This function connects to a DuckDB database, retrieves all stock symbols,
#' and filters them based on their respective indices (ASX, B3, SP500, Benchmark).
#'
#' @param con A valid DBI connection object to the DuckDB database.
#'
#' @return A list containing data frames for all symbols and filtered symbols by index.
#'
#' @import DBI
#' @import duckdb
#' @import dplyr
#' @export
#' @examples
#' \dontrun{
#' db_file <- "/mnt/nas_nuvens/stock_data/stock_data - Copia.duckdb"
#' # create a connection to the database
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_file, read_only = TRUE)
#' symbols <- load_symbols(con)
#' head(symbols$all_symbols_au)
#' head(symbols$all_symbols_br)
#' head(symbols$all_symbols_sp500)
#' head(symbols$all_benchmarks)
#' dbDisconnect(con)
#' }
#'
load_symbols <- function(con) {
  # # check the connection
  if (!grepl("connection", class(con))) {
    stop("Failed to connect to the database.")
  }

  # Check if the table 'all_symbols' exists
  if (!"all_symbols" %in% DBI::dbListTables(con)) {
    stop("Table 'all_symbols' does not exist in the database.")
  }
  # query the database to check if the data was written
  all_symbol_data = dbGetQuery(con, "SELECT * FROM all_symbols") %>%
    filter(symbol != "-")

  # Check if data was retrieved
  if (nrow(all_symbol_data) == 0) {
    stop("No data found in the 'all_symbols' table.")
  }

  # # get the symbols from AUS
  all_symbols_au <- all_symbol_data %>%
    filter(index == "ASX")

  # # get the symbols from BRA
  all_symbols_br <- all_symbol_data %>%
    filter(index == "B3")

  # Get the rest of the data for USA symbols
  all_symbols_sp500 <- all_symbol_data %>%
    filter(index == "SP500")

  # Get benchmarks for comparisons
  all_benchmarks <- all_symbol_data %>%
    filter(index == "Benchmark")

  # disconnect from the database
  dbDisconnect(con)

  return(list(
    all_symbol_data = all_symbol_data,
    all_symbols_au = all_symbols_au,
    all_symbols_br = all_symbols_br,
    all_symbols_sp500 = all_symbols_sp500,
    all_benchmarks = all_benchmarks
  ))
}

#' Fetch S&P 500 symbols and metadata
#'
#' Downloads and merges S&P 500 symbol data from multiple online sources:
#' - tidyquant::tq_index("SP500")
#' - Wikipedia
#' - StockAnalysis
#'
#' Cleans, standardizes, and returns a tibble of S&P 500 symbols.
#'
#' @import dplyr
#' @import tidyquant
#' @import rvest
#' @import stringr
#' @export
#' @examples
#' \dontrun{
#' sp500 <- scrap_sp500_symbols()
#' head(sp500)
#' }
#' @return A tibble with the following columns:
#' \describe{
#'   \item{symbol}{Ticker symbol (with no suffix)}
#'   \item{name}{Company name}
#'   \item{sector}{Standardized sector classification}
#'   \item{subsector}{Standardized subsector classification (or "Uncategorized")}
#'   \item{market_cap}{Market capitalization in USD}
#'   \item{estimated_tot_shares}{Approximate total shares outstanding}
#'   \item{index}{Index name ("SP500")}
#'   \item{rank}{Company rank within index (from MarketIndex)}
#'   \item{coin}{Currency of values ("USD")}
#'   \item{weight}{Relative weight based on market cap}
#'   \item{date_updated}{Date of data retrieval}
#'   \item{source}{Data source URL}
#' }
scrap_sp500_symbols <- function() {
  # ---- Helper: Parse market cap ----
  parse_market_cap <- function(x) {
    x <- gsub(",", "", x) # remove commas
    ifelse(
      grepl("B", x),
      as.numeric(readr::parse_number(x)) * 1e9,
      ifelse(
        grepl("M", x),
        as.numeric(readr::parse_number(x)) * 1e6,
        NA_real_
      )
    )
  }

  # ---- 1. Base list from tidyquant ----
  sp500_base <- tidyquant::tq_index("SP500") %>%
    filter(!(local_currency %in% c("AUD", "BRL")), symbol != "-")

  # ---- 2. Wikipedia scrape ----
  wiki_url <- "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
  wiki_table <- wiki_url %>%
    read_html() %>%
    html_node("table") %>%
    html_table(fill = TRUE) %>%
    dplyr::mutate(Symbol = gsub("\\.", "-", Symbol))

  # ---- 3. StockAnalysis scrape ----
  sa_url <- "https://stockanalysis.com/list/sp-500-stocks/"
  sa_table <- sa_url %>%
    read_html() %>%
    html_node("table") %>%
    html_table(fill = TRUE) %>%
    dplyr::mutate(Symbol = gsub("\\.", "-", Symbol))

  # ---- 4. Merge datasets ----
  df <- sp500_base %>%
    left_join(wiki_table, by = c("symbol" = "Symbol")) %>%
    left_join(sa_table, by = c("symbol" = "Symbol")) %>%
    arrange(desc(weight))

  # ---- 5. Cleaning and standardization ----
  df <- df %>%
    mutate(
      market_cap = parse_market_cap(`Market Cap`),
      price = `Stock Price`,
      estimated_tot_shares = shares_held,
      coin = local_currency,
      date_updated = as.Date(Sys.Date()),
      source = paste(wiki_url, sa_url, sep = "; "),
      index = "SP500",
      name = company,
      sector = `GICS Sector`,
      subsector = `GICS Sub-Industry`,
      rank = dplyr::row_number()
    ) %>%
    select(
      symbol,
      name,
      sector,
      subsector,
      market_cap,
      estimated_tot_shares,
      index,
      rank,
      coin,
      weight,
      date_updated,
      source
    ) %>%
    # ---- Sector & subsector recoding ----
    mutate(
      sector = dplyr::case_when(
        sector %in% c("Consumer Non Cyclical", "Consumer Staples") ~
          "Consumer Staples",
        sector %in% c("Consumer Cyclical", "Consumer Discretionary") ~
          "Consumer Discretionary",
        sector %in% c("Oil, Gas and Biofuels", "Energy") ~ "Energy",
        sector %in% c("Basic Materials", "Materials") ~ "Materials",
        sector %in% c("Health", "Health Care") ~ "Health Care",
        sector %in% c("Financial", "Financials") ~ "Financials",
        sector %in% c("Communications", "Communication Services") ~
          "Communication Services",
        sector == "Capital Goods and Services" ~ "Industrials",
        TRUE ~ sector
      ),
      subsector = dplyr::case_when(
        subsector == "Mining" ~ "Metals & Mining",
        subsector == "Oil, Gas and Biofuels" ~
          "Oil & Gas Exploration & Production",
        subsector == "Financial Intermediaries" ~ "Diversified Financials",
        subsector ==
          "Medical and Hospitalar Services, Analysis and Diagnostics" ~
          "Health Care Services",
        subsector == "Retail and Distribution" & sector == "Health Care" ~
          "Health Care Distributors",
        subsector == "Retail and Distribution" & sector == "Consumer Staples" ~
          "Food & Staples Retailing",
        subsector == "Steel and Metalurgy" ~ "Steel",
        subsector == "Transportation Equipment and Components" ~
          "Transportation Equipment",
        subsector == "Diversified" & sector == "Consumer Discretionary" ~
          "Broadline Retail",
        is.na(subsector) ~ "Uncategorized",
        TRUE ~ subsector
      )
    )

  return(df)
}

#' Get ASX-listed company data
#'
#' Scrapes and parses company data from the MarketIndex ASX listed companies page.
#'
#' The function launches a Selenium-driven Firefox browser to load the dynamic
#' table, extracts company information (symbol, name, sector, market cap, etc.),
#' and cleans the data into a standardized tibble.
#'
#' @import RSelenium
#' @import rvest
#' @import dplyr
#'
#' @return A tibble with the following columns:
#' \describe{
#'   \item{symbol}{Ticker symbol (with `.AX` suffix)}
#'   \item{name}{Company name}
#'   \item{sector}{Standardized sector classification}
#'   \item{subsector}{Standardized subsector classification (or "Uncategorized")}
#'   \item{market_cap}{Market capitalization in AUD}
#'   \item{estimated_tot_shares}{Approximate total shares outstanding}
#'   \item{index}{Index name ("ASX")}
#'   \item{rank}{Company rank within index (from MarketIndex)}
#'   \item{coin}{Currency of values ("AUD")}
#'   \item{weight}{Relative weight based on market cap}
#'   \item{date_updated}{Date of data retrieval}
#'   \item{source}{Data source URL}
#' }
#'
#' @examples
#' \dontrun{
#'   asx_data <- scrap_asx_symbols()
#'   head(asx_data)
#' }
#'
#' @export
scrap_asx_symbols <- function() {
  # URL of the ASX listed companies page. Can be overridden for testing.
  url <- "https://www.marketindex.com.au/asx-listed-companies"

  # kill any existing selenium or geckodriver processes
  system("pkill -f selenium", ignore.stdout = TRUE, ignore.stderr = TRUE)
  system("pkill -f geckodriver", ignore.stdout = TRUE, ignore.stderr = TRUE)

  # start Selenium driver
  driver <- RSelenium::rsDriver(
    browser = "firefox",
    port = 4445L,
    chromever = NULL,
    phantomver = NULL # without it I was running in this error: https://stackoverflow.com/questions/79653647/rselenium-error-in-open-connectioncon-rb-with-rsdriver
  )
  remDr <- driver$client

  tryCatch(
    {
      # navigate and wait for table
      remDr$navigate(url)
      Sys.sleep(10) # allow JS to load
      remDr$findElement(using = "css selector", "table.mi-table.mt-6")

      # extract page
      page_source <- remDr$getPageSource()[[1]]
      page <- read_html(page_source)

      # parse table
      asx_table <- page %>%
        html_nodes("table.mi-table.mt-6") %>%
        html_table(fill = TRUE)

      # clean data
      valid_stocks_au <- asx_table[[1]] %>%
        select(-c(2, 6, 7, 8)) %>%
        mutate(
          symbol = paste0(Code, ".AX"),
          market_cap = as.numeric(gsub("[BMK$]", "", `Mkt Cap`)) *
            case_when(
              grepl("B", `Mkt Cap`) ~ 1e9,
              grepl("M", `Mkt Cap`) ~ 1e6,
              grepl("K", `Mkt Cap`) ~ 1e3,
              TRUE ~ 1
            ),
          price = as.numeric(gsub("\\$|,", "", Price)), # remove $ and , from price
          estimated_tot_shares = market_cap / price,
          coin = "AUD",
          weight = market_cap / sum(market_cap, na.rm = TRUE),
          date_updated = as.Date(Sys.Date()),
          source = url,
          index = "ASX",
          subsector = as.character(NA)
        ) %>%
        select(
          symbol,
          name = Company,
          sector = Sector,
          subsector,
          market_cap,
          estimated_tot_shares,
          index,
          rank = Rank,
          coin,
          weight,
          date_updated,
          source
        ) %>%
        # standardize sector & subsector
        mutate(
          sector = case_when(
            sector %in% c("Consumer Non Cyclical", "Consumer Staples") ~
              "Consumer Staples",
            sector %in% c("Consumer Cyclical", "Consumer Discretionary") ~
              "Consumer Discretionary",
            sector %in% c("Oil, Gas and Biofuels", "Energy") ~ "Energy",
            sector %in% c("Basic Materials", "Materials") ~ "Materials",
            sector %in% c("Health", "Health Care") ~ "Health Care",
            sector %in% c("Financial", "Financials") ~ "Financials",
            sector %in% c("Communications", "Communication Services") ~
              "Communication Services",
            sector == "Capital Goods and Services" ~ "Industrials",
            TRUE ~ sector
          ),
          subsector = case_when(
            subsector == "Mining" ~ "Metals & Mining",
            subsector == "Oil, Gas and Biofuels" ~
              "Oil & Gas Exploration & Production",
            subsector == "Financial Intermediaries" ~ "Diversified Financials",
            subsector ==
              "Medical and Hospitalar Services, Analysis and Diagnostics" ~
              "Health Care Services",
            subsector == "Retail and Distribution" & sector == "Health Care" ~
              "Health Care Distributors",
            subsector == "Retail and Distribution" &
              sector == "Consumer Staples" ~
              "Food & Staples Retailing",
            subsector == "Steel and Metalurgy" ~ "Steel",
            subsector == "Transportation Equipment and Components" ~
              "Transportation Equipment",
            subsector == "Diversified" & sector == "Consumer Discretionary" ~
              "Broadline Retail",
            is.na(subsector) ~ "Uncategorized",
            TRUE ~ subsector
          )
        )

      return(valid_stocks_au)
    },
    error = function(e) {
      warning("Error while scraping ASX data: ", e$message)
      warning("Returning NULL.")
      return(NULL)
    },
    finally = {
      # ensure browser closes
      remDr$close()
      driver$server$stop()
    }
  )
}

#' Fetch B3 (IBOV) listed companies data
#'
#' This function automates downloading the daily IBOV CSV from B3,
#' parses it, joins with a local classification CSV, and returns a cleaned tibble.
#' It does not write to any database; you can handle storage outside.
#'
#' @param classification If character, should give the path to the classification CSV file (sector/subsector). If null, should use the internal classificacao_b3 dataset. Defaults to the package dataset classificacao_b3.
#' @param download_dir Directory to save the downloaded CSV. Defaults to "Downloads" folder.
#' @return A tibble with the following columns:
#' \describe{
#'   \item{symbol}{Ticker symbol (with `.SA` suffix)}
#'   \item{name}{Company name}
#'   \item{sector}{Standardized sector classification}
#'   \item{subsector}{Standardized subsector classification (or "Uncategorized")}
#'   \item{market_cap}{Market capitalization in BRL}
#'   \item{estimated_tot_shares}{Approximate total shares outstanding}
#'   \item{index}{Index name ("B3")}
#'   \item{rank}{Company rank within index (from MarketIndex)}
#'   \item{coin}{Currency of values ("BRL")}
#'   \item{weight}{Relative weight based on market cap}
#'   \item{date_updated}{Date of data retrieval}
#'   \item{source}{Data source URL}
#' }
#' @import RSelenium
#' @import rvest
#' @import readr
#' @import dplyr
#' @export
scrap_b3_symbols <- function(
  class_file = NULL,
  download_dir = "Downloads"
) {
  # Load internal classificacao_b3 dataset if no path provided
  if (is.null(class_file)) {
    b3_class <- classificacao_b3
  } else if (!file.exists(class_file)) {
    stop("Classification file not found at the provided path.")
  } else if (!grepl("\\.csv$", class_file)) {
    stop("Classification file must be a CSV.")
  } else {
    # Load classification CSV
    b3_class <- readr::read_csv(class_file)
    # check for mandatory columns
    required_cols <- c(
      "symbol",
      "company_name",
      "sector",
      "subsector",
      "segment"
    )
    if (!all(required_cols %in% colnames(b3_class))) {
      stop(paste(
        "Classification CSV must contain columns:",
        paste(required_cols, collapse = ", ")
      ))
    }
  }

  # Kill any existing selenium or geckodriver processes
  system("pkill -f selenium", ignore.stdout = TRUE, ignore.stderr = TRUE)
  system("pkill -f geckodriver", ignore.stdout = TRUE, ignore.stderr = TRUE)

  # Start RSelenium
  driver <- rsDriver(
    browser = "firefox",
    port = 4446L,
    chromever = NULL,
    phantomver = NULL
  )
  remDr <- driver$client
  on.exit(
    {
      try(remDr$close(), silent = TRUE)
      try(driver$server$stop(), silent = TRUE)
    },
    add = TRUE
  )

  # Navigate to B3 IBOV page
  url <- "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=en-us"
  remDr$navigate(url)
  Sys.sleep(10)

  # Click the download link
  download_link <- remDr$findElement(using = "link text", "Download")
  download_link$clickElement()

  # find out the complete download path. If windows or linux
  download_dir <- ifelse(
    grepl("windows", tolower(Sys.info()[["sysname"]])),
    file.path(Sys.getenv("USERPROFILE"), download_dir),
    file.path(Sys.getenv("HOME"), download_dir)
  )

  # Wait dynamically for file download

  wait_for_file <- function(pattern, dir = download_dir, timeout = 30) {
    start <- Sys.time()
    repeat {
      files <- list.files(dir, pattern = pattern, full.names = TRUE)
      if (length(files) > 0) {
        return(files[[1]])
      }
      if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
        stop("Timeout waiting for B3 CSV download")
      }
      Sys.sleep(1)
    }
  }

  file_path <- wait_for_file(pattern = "IBOVDia", download_dir)

  # Read downloaded CSV
  # suppress warnings about parsing failures
  b3_list <- suppressWarnings(
    readr::read_csv(file_path, skip = 1) %>%
      slice(1:(n() - 2)) %>% # remove last 2 summary rows
      rename(BRX.code = Code) %>%
      mutate(BRX.code = paste0(BRX.code, ".SA"))
  )

  # Remove the file
  file.remove(file_path)

  # Join and clean
  valid_stocks_br <- b3_list %>%
    rename(symbol = BRX.code) %>%
    mutate(link = gsub("[0-9]+\\.SA$", "", symbol)) %>%
    left_join(b3_class, by = c("link" = "symbol")) %>%
    arrange(desc(`Part. (%)`)) %>%
    mutate(
      market_cap = `Theoretical Quantity`, # price not available here; calculate externally
      price = NA_real_,
      estimated_tot_shares = `Theoretical Quantity`,
      coin = "BRL",
      weight = `Part. (%)` / 100,
      date_updated = as.Date(Sys.Date()),
      source = url,
      index = "B3",
      name = coalesce(company_name, Stock),
      rank = row_number()
    ) %>%
    select(
      symbol,
      name,
      sector,
      subsector,
      market_cap,
      estimated_tot_shares,
      price,
      coin,
      weight,
      date_updated,
      source,
      index,
      rank
    ) %>%
    # Standardize sector/subsector
    mutate(
      sector = case_when(
        sector %in% c("Consumer Non Cyclical", "Consumer Staples") ~
          "Consumer Staples",
        sector %in% c("Consumer Cyclical", "Consumer Discretionary") ~
          "Consumer Discretionary",
        sector %in% c("Oil, Gas and Biofuels", "Energy") ~ "Energy",
        sector %in% c("Basic Materials", "Materials") ~ "Materials",
        sector %in% c("Health", "Health Care") ~ "Health Care",
        sector %in% c("Financial", "Financials") ~ "Financials",
        sector %in% c("Communications", "Communication Services") ~
          "Communication Services",
        sector == "Capital Goods and Services" ~ "Industrials",
        TRUE ~ sector
      ),
      subsector = case_when(
        subsector == "Mining" ~ "Metals & Mining",
        subsector == "Oil, Gas and Biofuels" ~
          "Oil & Gas Exploration & Production",
        subsector == "Financial Intermediaries" ~ "Diversified Financials",
        subsector ==
          "Medical and Hospitalar Services, Analysis and Diagnostics" ~
          "Health Care Services",
        subsector == "Retail and Distribution" & sector == "Health Care" ~
          "Health Care Distributors",
        subsector == "Retail and Distribution" & sector == "Consumer Staples" ~
          "Food & Staples Retailing",
        subsector == "Steel and Metalurgy" ~ "Steel",
        subsector == "Transportation Equipment and Components" ~
          "Transportation Equipment",
        subsector == "Diversified" & sector == "Consumer Discretionary" ~
          "Broadline Retail",
        is.na(subsector) ~ "Uncategorized",
        TRUE ~ subsector
      )
    )

  return(valid_stocks_br)
}
