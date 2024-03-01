#' @title Download price time-series from Tiingo
#' @param ticker character of stock ticker to download, see
#'   `download_tiingo_tickers` for downloading multiple tickers
#' @param t_api API key
#' @param date_start inception for time-series
#' @param date_end last day of time-series, if left `NULL` will default to today
#' @return `data.frame` of historical prices (adjusted for splits and dividends)
#'   with dates in first column
#' @export
download_tiingo_csv <- function(ticker, t_api, date_start = '1970-01-01',
                                date_end = NULL) {
  if (is.null(date_end)) {
    date_end <- Sys.Date()
  }
  t_url <- paste0('https://api.tiingo.com/tiingo/daily/',
                  ticker,
                  '/prices?startDate=', date_start,
                  '&endDate=', date_end,
                  '&format=csv&resampleFreq=daily',
                  '&token=', t_api)
  dat <- try(read.csv(t_url), silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' was not downloaded'))
    return(NULL)
  }
  print(paste0(ticker, ' downloaded'))
  dat <- try(dat[, c('date', 'adjClose')], silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' columns not found'))
    return(NULL)
  }
  colnames(dat)[2] <- ticker
  return(dat)
}


#' @title Download multiple tickers from Tiingo
#' @param ticker_vec character vector of tickers to download
#' @param t_api API key
#' @param date_start first date in time-series to download
#' @param date_end last date in time-series to download, if left `NULL` will
#'   default to today
#' @return xts of price time-series adjusted for dividends and splits
#' @examples
#'   \dontrun{
#'   download_tiingo_tickers(c('AAPL', 'GOOG', 'TSLA'), api_key, '2020-01-01')
#' }
#' @export
download_tiingo_tickers <- function(ticker_vec, t_api, date_start, date_end = NULL) {

  utick <- unique(ticker_vec)
  if (length(utick) < length(ticker_vec)) {
    warning('duplicated tickers found and removed')
  }
  dat <- lapply(utick, download_tiingo_csv, t_api = t_api,
                date_start = date_start, date_end = date_end)
  dat <- dat[!sapply(dat, is.null)]
  nm <- sapply(dat, function(x) {colnames(x)[2]})
  if (length(dat) == 1) {
    price <- dat[[1]]
  } else {
    dt <- us_trading_days(date_start, date_end)
    price <- matrix(nrow = length(dt), ncol = length(dat))
    for (i in 1:ncol(price)) {
      ix <- match(dat[[i]]$date, dt)
      price_on_holiday <- is.na(ix)
      price[na.omit(ix), i] <- dat[[i]][[2]][!price_on_holiday]
      if (i %% 100 == 0) print(paste0(i, ' out of ', length(dat)))
    }
  }
  price_df <- data.frame('date' = dt, price)
  colnames(price_df) <- c('date', nm)
  return(price_df)
}


#' @title Download from FED Database
#' @param series_id character to represent the series you want to download
#' @param fred_api API key, see https://fred.stlouisfed.org/ to create one
#' @return xts of economic time-series
#' @export
download_fred <- function(series_id, fred_api) {
  f_url <- paste0('https://api.stlouisfed.org/fred/series/observations?series_id=',
                  series_id, '&api_key=', fred_api, '&file_type=json')
  request <- httr::GET(f_url)
  dat <- jsonlite::parse_json(request)
  dt <- sapply(dat$observations, '[[', 'date')
  obs <- sapply(dat$observations, '[[', 'value')
  x <- xts(as.numeric(obs), as.Date(dt))
  colnames(x) <- series_id
  return(x)
}




