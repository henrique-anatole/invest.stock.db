#' Reference data for B3 sector classification
#'
#' A dataset containing the sector classification of companies listed on B3, translated to English and standardized for consistency.
#'
#' @format data.frame with 6 columns:
#' \describe{
#'  \item{sector}{Sector of the company}
#' \item{subsector}{Subsector of the company}
#' \item{segment}{Segment of the company}
#' \item{company_name}{Name of the company}
#' \item{symbol}{Stock ticker symbol}
#' \item{segment_code}{B3 Code of the segment}
#' }
#' @source \url{https://www.b3.com.br/en_us/products-and-services/trading/equities/equities/reports/industry-classification/}
#'
#' @details
#' This dataset is not updated regularly. It is used to classify brazilian stocks into sectors, subsectors, and segments based on B3's official classification.
#' For the most current classification, refer to the official B3 website.
#'
"classificacao_b3"

#' Sample of stock symbols data
#'
#' A sample dataset containing stock symbols and related information for randomly selected companies.
#' Its used for testing and demonstration purposes.
#'
#'
"sample_all_symbols"

#' Fake page content for testing
#'
#' A sample HTML content used for testing web scraping functions.
#' It simulates a webpage structure with stock symbols and company names.
#'
#'
"fake_page"
