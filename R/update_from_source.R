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
#' @param date_start first date in time-series to download, need to be Date class,
#'   e.g., as.Date('2024-01-01')
#' @param date_end last date in time-series to download, if left `NULL` will
#'   default to today
#' @param out_ret boolean if TRUE outputs returns, if FALSE outputs prices
#' @param out_xts boolean, if TRUE outputs xts object, if FALSE returns data.frame
#' @return xts of price time-series adjusted for dividends and splits
#' @export
download_tiingo_tickers <- function(ticker_vec, t_api, date_start = NULL,
                                    date_end = NULL, out_ret = FALSE,
                                    out_xts = FALSE) {

  if (is.null(date_start)) {
    date_start <- as.Date('1970-01-01')
  }
  utick <- unique(ticker_vec)
  if (length(utick) < length(ticker_vec)) {
    warning('duplicated tickers found and removed')
  }
  dat <- lapply(utick, download_tiingo_csv, t_api = t_api,
                date_start = date_start, date_end = date_end)
  dat <- dat[!sapply(dat, is.null)]
  if (length(dat) == 0) {
    warning('no tickers found')
    return()
  }
  nm <- sapply(dat, function(x) {colnames(x)[2]})
  if (length(dat) == 1) {
    price <- dat[[1]][, 2]
    dt <- dat[[1]][, 1]
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
  if (out_ret) {
    res <- mat_to_xts(price_df)
    res <- price_to_ret(res)
    colnames(res) <- nm
    if (!out_xts) {
      res <- xts_to_dataframe(res)
    }
  } else {
    res <- price_df
    if (out_xts) {
      res <- mat_to_xts(price_df)
      colnames(res) <- nm
    }
  }
  return(res)
}


#' @title Download Econ Time-series from FED Database
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


#' @title Download Holdings from SEC Database
#' @param long_cik string to represent the fund / share-class CIK, typically longer in length
#' @param short_cik string to represent the parent company CIK, typically shorter in length
#' @param user_email SEC's API requires an email address
#' @param as_of optional date to mark when the download occured
#' @return data.frame with fund holdings
#' @export
download_sec_nport = function(long_cik, short_cik, user_email) {
  doc_type <- 'nport-p'
  url <- paste0('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=',
                long_cik,
                '&type=', doc_type,
                '&dateb=&count=5&scd=filings&search_text=')
  # SEC requires email as a header for authentication, you don't have to register
  # email on their site, just need valid email address
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  html_doc <- rvest::read_html(response)
  tbl <- rvest::html_table(html_doc)
  # might need to change which table to target if web design changes
  long_str <- tbl[[3]]$Description[1]
  file_date <- tbl[[3]]$`Filing Date`[1]
  # need to extract ##-##-## from table cell to target the url of the latest filing
  num_id <- stringr::str_extract(long_str, '[[:digit:]]+-[[:digit:]]+-[[:digit:]]+')
  num_str <- gsub('-', '', num_id)
  # now we can target url of latest filing and get the holdings
  url <- paste0('https://www.sec.gov/Archives/edgar/data/',
                short_cik, '/', num_str, '/primary_doc.xml')
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  xml_file <- xml2::read_xml(response)
  doc <- XML::xmlParse(xml_file)
  root <- XML::xmlRoot(doc)
  # xmlToList can take ~10+ seconds for large amount of holdings, is there more efficient way?
  all_list <- XML::xmlToList(root)
  # current node is invstOrSecs for the holdings
  all_sec <- all_list$formData$invstOrSecs
  df <- data.frame(
    name = NA,
    lei = NA,
    title = NA,
    cusip = NA,
    isin = NA, # conditional, required to list at least one additional ID
    altid = NA, # ID is usually ISIN, check for ISIN otherwise call alt
    balance = NA,
    units = NA,
    curCd = NA,
    exchangeRt = NA, # conditional, if no FX then this field doesn't exist
    valUSD = NA,
    pctVal = NA,
    payoffProfile = NA,
    assetCat = NA,
    issuerCat = NA,
    invCountry = NA,
    returnInfo = NA
  )
  for (i in 1:length(all_sec)) {
    x <- all_sec[[i]]
    df[i, 'name'] <- x$name
    df[i, 'lei'] <- x$lei
    df[i, 'title'] <- x$title
    df[i, 'cusip']  <- x$cusip
    if ('isin' %in% names(x$identifiers)) {
      df[i, 'isin'] <- x$identifiers$isin
      df[i, 'altid'] <- NA
    } else {
      df[i, 'isin'] <- NA
      df[i, 'altid'] <- x$identifiers[[1]][2]
    }
    df[i, 'balance'] <- x$balance
    df[i, 'units'] <- x$units
    if ('curCd' %in% names(x)) {
      df[i, 'curCd'] <- x$curCd
      df[i, 'exchangeRt'] <- NA
    } else if ('currencyConditional' %in% names(x)) {
      df[i, 'curCd'] <- x$currencyConditional[1]
      df[i, 'exchangeRt'] <- x$currencyConditional[2]
    } else {
      df[i, 'curCd'] <- NA
      df[i, 'exchangeRt'] <- NA
    }
    df[i, 'valUSD'] <- x$valUSD
    df[i, 'pctVal'] <- x$pctVal
    df[i, 'payoffProfile'] <- x$payoffProfile
    if ('assetCat' %in% names(x)) {
      df[i, 'assetCat'] <- x$assetCat
    } else if ('assetConditional' %in% names(x)) {
      df[i, 'assetCat'] <- x$assetConditional[2]
    } else {
      df[i, 'assetCat'] <- NA
    }
    if ('issuerCat' %in% names(x)) {
      df[i, 'issuerCat'] <- x$issuerCat[1]
    } else {
      df[i, 'issuerCat'] <- NA
    }
    df[i, 'invCountry'] <- x$invCountry
  }
  df[ ,'returnInfo'] <- as.Date(file_date)
  df$pctVal <- as.numeric(df$pctVal)
  df$pctVal <- df$pctVal / 100
  return(df)
}


#' @title Download Holdings from BlackDiamond
#' @param account_id string to represent target account (internal ID)
#' @param as_of date to pull holdings as of, latest is last business day
#' @return data.frame with holdings
#' @export
download_bd <- function(account_id, api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  bd_key <- api_keys$bd_key
  response <- httr::POST(
    'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
    accept_json(),
    add_headers(
      Authorization = paste0('Bearer ', bd_key$refresh_token),
      `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
    ),
    encode = 'json',
    body = list(accountID = account_id, asOf = as_of,
                onlyCurrentHoldings = TRUE, limit = 100000,
                include = list(returnInfo = TRUE, assets = TRUE))
  ) # end POST
  if (response$status_code != 200) {
    bd_key <- refresh_bd_key(api_keys, TRUE)
    response <- httr::POST(
      'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
      accept_json(),
      add_headers(
        Authorization = paste0('Bearer ', bd_key$refresh_token),
        `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
      ),
      encode = 'json',
      body = list(accountID = account_id, asOf = as_of,
                  onlyCurrentHoldings = TRUE, limit = 100000,
                  include = list(returnInfo = TRUE, assets = TRUE))
    ) # end POST
  }
  # parse jason and extract data
  rd <- parse_json(response)
  rd <- rd$data
  if (length(rd) == 0) {
    warning('no data found')
    return(NULL)
  }
  # convert json list into data.frame by looping through each holding
  df <- data.frame(
    assetId = NA,
    name = NA,
    cusip = NA,
    ticker = NA,
    identifier = NA,
    units = NA,
    emv = NA,
    returnInfo = NA
  )
  robcheck <- function(x, l) {
    if (x %in% names(l)) {
      if (!is.null(l[[x]])) {
        return(l[x])
      } else {
        return(NA)
      }
    } else {
      return(NA)
    }
  }
  for (i in 1:length(rd)) {
    df[i, 'assetId'] <- rd[[i]]$asset['assetId'][[1]]
    df[i, 'name'] <- rd[[i]]$asset['name'][[1]]
    df[i, 'cusip'] <- robcheck('cusip', rd[[i]]$asset)
    df[i, 'ticker'] <- robcheck('ticker', rd[[i]]$asset)
    df[i, 'identifier'] <- robcheck('identifier', rd[[i]]$asset)
    df[i, 'units'] <- rd[[i]]$returnInfo['units'][[1]]
    df[i, 'emv'] <- rd[[i]]$returnInfo['emv'][[1]]
    df[i, 'returnInfo'] <- rd[[i]]$returnInfo['returnDate'][[1]]
  }
  df$pctVal <- df$emv / sum(df$emv, na.rm = TRUE)
  return(df)
}


#' @export
refresh_bd_key = function(api_keys, save_to_n = FALSE) {
  bd_key <- api_keys$bd_key
  response <- httr::POST('https://login.bdreporting.com/connect/token',
                         encode = 'form',
                         body = list(grant_type = 'password',
                                     client_id = bd_key$client_id,
                                     client_secret = bd_key$client_secret,
                                     username = bd_key$username,
                                     password = bd_key$password))
  tk <- jsonlite::parse_json(response)
  bd_key$refresh_token <- tk$access_token
  if (save_to_n) {
    api_keys$bd_key <- bd_key
    save(api_keys, file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData')
  }
  return(bd_key)
}


#' @title Download Daily Returns from Factset
#' @param id factset ID, e.g., "CLIENT:/PA_SOURCED_RETURNS/DTC_US_ACTIVE_EQUITY_COMMON_FUND"
#' @param api_keys list with API keys
#' @param t_minus character to represent how many days to look back for time-series,
#'   default "-5" downloads last 5 trading days
#' @return list with id and xts of returns
#' @export
download_fs_ret <- function(id, api_keys, t_minus = "-5") {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  base_url <- 'https://api.factset.com/formula-api/v1/time-series?ids=$IDS&formulas='
  request <- paste0(
    base_url,
    'RA_RET(',
    "\"",
    id,
    "\",",
    t_minus,
    ",0,D,FIVEDAY,USD,1)"
  )
  response <- httr::GET(request, authenticate(username, password))
  output <- rawToChar(response$content)
  dat <- parse_json(output)
  dt <- unlist(dat$data[[1]]$result$dates)
  val <- unlist(dat$data[[1]]$result$values) / 100
  ret <- xts(val, as.Date(dt))
  return(list(id = id, ret = ret))
}

#' @title Factset Formula API
#' @param api_keys list with API keys
#' @param ids string vector with ids, e.g., ticker, CUSIP, ISIN
#' @param formulas string vector with formulas, see details
#' @param features string vector describing formula results, see details
#' @return data.frame with data
#' @details Formulas are factset API formulas, e.g., "P_PRICE(0, -2, D)". You
#'   can list multiple formulas as strings:
#'   c("P_PRICE(0, -2, D)", "FF_EPS(QTR_R, 0)"). Features are a description to
#'   match results of formulas, in this example c("Price", "EPS").
#' @export
download_fs <- function(api_keys, ids, formulas, type = c('ts', 'cs')) {
  if (type[1] == 'ts') {
    struc <- 'time-series'
  } else {
    struc <- 'cross-sectional'
  }
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  request <- paste0(
    "https://api.factset.com/formula-api/v1/",
    struc,
    "?ids=",
    paste0(ids, collapse = ","),
    "&formulas=",
    paste0(formulas, collapse = ",")
  )
  response <- httr::GET(request, authenticate(username, password))
  print(response$status)
  output <- rawToChar(response$content)
  #json <- fromJSON(output)[["data"]]
  json <- parse_json(output)
  return(json)
}


#' @export
download_fs_large_ids <- function(api_keys, ids, formulas) {
  if (ids < 100) {
    warning("function is for over 100 ids, returning NULL")
    return()
  }
  ids <- na.omit(unique(ids))
  iter <- seq(1, length(ids), 100)
  iter[length(iter)] <- length(ids)
  json <- download_fs(api_keys, ids[1:100], formulas, 'cs')
  df <- unlist_fs_cs(json)
  for (i in 2:(length(iter)-1)) {
    json_i <- download_fs(api_keys, ids[iter[i]:iter[i+1]], formulas)
    df_i <- unlist_fs_cs(json_i)
    df <- rob_rbind(df, df_i)
    print(iter[i])
  }
}



#' @title Transform FactSet time-series JSON into data.frame
#' @param json json list output from `download_fs`
#' @return data.frame
#' @export
unlist_fs_ts <- function(json) {
  dat <- json$data
  res <- lapply(dat, '[[', 'result')
  dt <- res[[1]]$dates[[1]][[1]]
  val <- res[[1]]$values[[1]][[1]]
  for (i in 2:length(res)) {
    dt_i <- try(res[[i]]$dates[[1]][[1]])
    if (is.null(dt_i) | "try-error" %in% class(dt_i)) dt_i <- NA
    dt <- c(dt, dt_i)
    val_i <- try(res[[i]]$values[[1]][[1]])
    if (is.null(val_i) | "try-error" %in% class(val_i)) val_i <- NA
    val <- c(val, val_i)
  }
  df <- data.frame(
    dates = dt,
    values = val,
    requestId = unlist(lapply(dat, '[[', 'requestId')),
    formulas = unlist(lapply(dat, '[[', 'formula'))
  )
  return(df)
}


#' @export
unlist_fs_cs <- function(json) {
  dat <- json$data
  res <- lapply(dat, '[[', 'result')
  x <- NULL
  for (i in 1:length(res)) {
    if (!is.null(res[[i]])) {
      x <- i
      break
    }
  }
  if (is.null(x)) {
    stop('all json$data$result is NULL')
  }
  if ('list' %in% class(res[[x]][[1]])) {
    requestId <- lapply(dat, '[[', 'requestId')
    requestId[sapply(requestId, is.null)] <- NA
    values <- lapply(res, function(x){x$values[[1]]})
    values[sapply(values, is.null)] <- NA
    df <- data.frame(requestId = unlist(requestId), values = unlist(values))
  } else {
    res[[1]][sapply(res[[1]], is.null)] <- NA
    res[[2]][sapply(res[[2]], is.null)] <- NA
    df <- data.frame(requestId = unlist(res[[1]]), values = unlist(res[[2]]))
  }
  return(df)
}


#' @title Download company name of from large number of ids
#' @param api_keys list with api_keys, see Database
#' @param ids requestIds
#' @export
download_fs_large_names <- function(api_keys, ids) {
  iter <- seq(1, length(ids), 100)
  iter[length(iter)] <- length(ids)
  json <- download_fs(api_keys, ids[1:100], 'FF_CO_NAME', 'cs')
  df <- unlist_fs_cs(json)
  colnames(df) <- c('requestId', 'FFCompanyName')
  for (i in 2:(length(iter)-1)) {
    json_i <- download_fs(api_keys, ids[iter[i]:iter[i+1]], "FF_CO_NAME")
    df_i <- unlist_fs(json_i)
    colnames(df_i) <- c('requestId', 'FFCompanyName')
    df <- rob_rbind(df, df_i)
    print(iter[i])
  }
  return(df)
}

