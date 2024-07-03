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
download_fs_ctf_ret <- function(id, api_keys, t_minus = "-5") {
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
  dt <- dat$data[[1]]$result$dates
  dt[sapply(dt, is.null)] <- NA
  val <- dat$data[[1]]$result$values
  val[sapply(val, is.null)] <- NA
  ret <- xts(unlist(val), as.Date(unlist(dt)))
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


read_hfr_ror_file <- function(wb) {
  raw <- read.csv(wb)
  meta <- raw[, 1:7]
  dt <- gsub('X', '', colnames(raw)[8:ncol(raw)])
  dt <- ymd(paste0(dt, '.01'))
}

# EOD Archive ----

#' #' @export
#' eod_list_exchanges <- function(api_keys) {
#'   url <- paste0(
#'     'https://eodhd.com/api/exchanges-list/?api_token=',
#'     api_keys$eod_key,
#'     '&fmt=json'
#'   )
#'   r <- httr::GET(url)
#'   json <- jsonlite::parse_json(r)
#'   name <- extract_list(json, 'Name')
#'   code <- extract_list(json, 'Code')
#'   country <- extract_list(json, 'Country')
#'   iso2 <- extract_list(json, 'CountryISO2')
#'   data.frame(
#'     Name = name,
#'     Code = code,
#'     Country = country,
#'     ISO2 = iso2
#'   )
#' }
#'
#'
#' #' @export
#' eod_list_stocks <- function(api_keys, x_code) {
#'   url <- paste0(
#'     'https://eodhd.com/api/exchange-symbol-list/',
#'     x_code,
#'     '?api_token=',
#'     api_keys$eod_key,
#'     '&fmt=json'
#'   )
#'   r <- httr::GET(url)
#'   json <- jsonlite::parse_json(r)
#'   code <- extract_list(json, 'Code')
#'   name <- extract_list(json, 'Name')
#'   country <- extract_list(json, 'Country')
#'   exchange <- extract_list(json, 'Exchange')
#'   currency <- extract_list(json, 'Currency')
#'   type <- extract_list(json, 'Type')
#'
#'   data.frame(
#'     Code = unlist(code),
#'     Name = unlist(name),
#'     Country = unlist(country),
#'     Exchange = unlist(exchange),
#'     Currency = unlist(currency),
#'     Type = unlist(type)
#'   )
#' }
#'
#'
#' #' @export
#' eod_total_ret <- function(api_keys, code_vec, country_code, date_start = NULL,
#'                           date_end = NULL, out_ret = FALSE, out_xts = FALSE) {
#'   if (is.null(date_start)) {
#'     date_start <- as.Date('1970-01-01')
#'   }
#'   if (is.null(date_end)) {
#'     date_end <- Sys.Date()
#'   }
#'   code_vec <- paste0(code_vec, '.', country_code)
#'   dat <- list()
#'   for (i in 1:length(code_vec)) {
#'     print(paste0(code_vec[i], ' ', i, ' out of ', length(code_vec)))
#'     url <- paste0(
#'       'https://eodhd.com/api/eod/',
#'       code_vec[i],
#'       '?from=', date_start,
#'       '&to=', date_end,
#'       '&period=d&api_token=',
#'       api_keys$eod_key,
#'       '&fmt=json'
#'     )
#'     r <- GET(url)
#'     if (r$status_code == 200) {
#'       json <- parse_json(r)
#'     } else {
#'       print(r$status_code)
#'       next
#'     }
#'     if (length(json) == 0) {
#'       dat[[i]] <- NA
#'       print(paste0(code_vec[i], ' is empty'))
#'     } else {
#'       dt <- sapply(json, '[[', 'date')
#'       price <- sapply(json, '[[', 'adjusted_close')
#'       x <- try(data.frame(as.Date(dt), price), silent = TRUE)
#'       if ('try-error' %in% class(x)) {
#'         print(paste0('df error'))
#'         dat[[i]] <- NA
#'         next
#'       }
#'       colnames(x) <- c('Date', code_vec[i])
#'       dat[[i]] <- x
#'     }
#'   }
#'   ix <- sapply(dat, function(x) {class(x)[1]})
#'   dat <- dat[ix == 'data.frame']
#'   if (length(dat) == 1) {
#'     price <- dat[[1]][, 2]
#'     dt <- dat[[1]][, 1]
#'     col_nm <- colnames(dat)[2]
#'   } else {
#'     dt <- us_trading_days(date_start, date_end)
#'     price <- matrix(nrow = length(dt), ncol = length(dat))
#'     col_nm <- sapply(dat, function(x){colnames(x)[2]})
#'     for (i in 1:ncol(price)) {
#'       ix <- try(match(dat[[i]]$Date, dt))
#'       price_on_holiday <- is.na(ix)
#'       price[na.omit(ix), i] <- dat[[i]][[2]][!price_on_holiday]
#'       if (i %% 100 == 0) print(paste0(i, ' out of ', length(dat)))
#'     }
#'   }
#'   price_df <- data.frame('date' = dt, price)
#'   colnames(price_df) <- c('date', col_nm)
#'   if (out_ret) {
#'     res <- mat_to_xts(price_df)
#'     res <- price_to_ret(res)
#'     colnames(res) <- col_nm
#'     if (!out_xts) {
#'       res <- xts_to_dataframe(res)
#'     }
#'   } else {
#'     res <- price_df
#'     if (out_xts) {
#'       res <- mat_to_xts(price_df)
#'       colnames(res) <- col_nm
#'     }
#'   }
#'   return(res)
#' }
#'
#'
#' #' @export
#' eod_general <- function(api_keys, s_df) {
#'   code_vec <- paste0(s_df$Code, '.', s_df$Exchange)
#'   df <- data.frame(Code = NA, Type = NA, Name = NA, Exchange = NA,
#'                    CurrencyCode = NA, CurrencyName = NA, CountryISO = NA,
#'                    CountryName = NA, OpenFigi = NA, ISIN = NA, LEI = NA,
#'                    PrimaryTicker = NA, CUSIP = NA, CIK = NA, Sector = NA,
#'                    Industry = NA)
#'   parse_error <- NA
#'   df_error <- NA
#'   for (i in 1:length(code_vec)) {
#'     print(paste0(code_vec[i], ' ', i, ' out of ', length(code_vec)))
#'     url <- paste0(
#'       'https://eodhd.com/api/fundamentals/',
#'         code_vec[i],
#'       '?filter=General&api_token=',
#'       api_keys$eod_key,
#'       '&fmt=json'
#'     )
#'     r <- GET(url)
#'     json <- try(parse_json(r), silent = TRUE)
#'     if ('try-error' %in% class(json)) {
#'       print(paste0(code_vec[i], ' parse error'))
#'       parse_error <- c(parse_error, code_vec[i])
#'       next
#'     }
#'     df_i <- try(data.frame(
#'       Code = check_null(json, 'Code'),
#'       Type = check_null(json, 'Type'),
#'       Name = check_null(json, 'Name'),
#'       Exchange = check_null(json, 'Exchange'),
#'       CurrencyCode = check_null(json, 'CurrencyCode'),
#'       CurrencyName = check_null(json, 'CountryName'),
#'       CountryISO = check_null(json, 'CountryISO'),
#'       CountryName = check_null(json, 'CountryName'),
#'       OpenFigi = check_null(json, 'OpenFigi'),
#'       ISIN = check_null(json, 'ISIN'),
#'       LEI = check_null(json, 'LEI'),
#'       PrimaryTicker = check_null(json, 'PrimaryTicker'),
#'       CUSIP = check_null(json, 'CUSIP'),
#'       CIK = check_null(json, 'CIK'),
#'       Sector = check_null(json, 'Sector'),
#'       Industry = check_null(json, 'Industry')
#'     ))
#'     if ('try-error' %in% class(df_i)) {
#'       print(paste0(code_vec[i], ' data.frame error'))
#'       df_error <- c(df_error, code_vec[i])
#'       next
#'     }
#'     df <- rbind(df, df_i)
#'   }
#'   res <- list()
#'   res$df <- df
#'   res$parse_error <- parse_error
#'   res$df_error <- df_error
#'   return(res)
#' }
#'
#'
#' #' @export
#' eod_fund <- function(api_keys, code_vec, country_code) {
#'   url <- paste0(
#'     'https://eodhd.com/api/fundamentals/',
#'     '000001.SHE',
#'     '?api_token=661ec0af0310e9.32152732&fmt=json'
#'   )
#'   r <- GET(url)
#'   json <- parse_json(r)
#' }
#'
#' #' @export
#' eod_list_country_stocks <- function(x_df, iso2, api_keys) {
#'   c_df <- x_df[x_df$ISO2 == iso2, ]
#'   s_df <- eod_list_stocks(api_keys = api_keys, x_code = c_df$Code[1])
#'   if (nrow(c_df) > 1) {
#'     for (i in 2:nrow(c_df)) {
#'       x <- eod_list_stocks(api_keys, c_df$Code[i])
#'       s_df <- rbind(s_df, x)
#'     }
#'   }
#' }
#'
#'
#' #' @export
#' check_null <- function(list, nm) {
#'   x <- list[[nm]]
#'   if (is.null(x)) {
#'     return(NA)
#'   } else {
#'     return(x)
#'   }
#' }
#'
#'
#' #' @export
#' read_eod_general <- function(bucket) {
#'   files <- db$bucket$ls('co-data/general')
#'   df <- read_parquet(bucket$path(files[1]))
#'   for (i in 2:length(files)) {
#'     x <- read_parquet(bucket$path(files[i]))
#'     df <- rbind(df, x)
#'   }
#'   return(df)
#' }
#'
#'
#' #' @export
#' eod_match <- function(df, eod_df) {
#'   incomp <- c(NA, "")
#'   ix_isin <- match(df$isin, eod_df$ISIN, incomparables = incomp)
#'   ix_ticker <- match(paste0(df$Ticker, '.', df$invCountry),
#'                      paste0(eod_df$Code, '.', eod_df$CountryISO),
#'                      incomparables = incomp)
#'   ix_cusip <- match(df$cusip, eod_df$CUSIP)
#'   ix_lei <- match(df$lei, eod_df$LEI)
#'   ix <- rep(NA, nrow(df))
#'   ix <- .fill_ix(ix, ix_isin)
#'   ix <- .fill_ix(ix, ix_ticker)
#'   ix <- .fill_ix(ix, ix_cusip)
#'   ix <- .fill_ix(ix, ix_lei)
#'   return(ix)
#' }
#'
#'
#' eod_select_download <- function(port_df, eod_gen_df) {
#'   ix <- eod_match(port_df, eod_gen_df)
#'   x <- cbind(port_df, eod_gen_df[ix, c('Code', 'Exchange')])
#'   miss <- x[is.na(x$Code), ]
#'   xmatch <- x[!is.na(x$Code), ]
#'   xmatch <- clean_us_exchange(xmatch)
#'   df <- data.frame(
#'     MarketCap = NA, EBITDA = NA, DividendShare = NA, EarningsShare = NA,
#'     ProfitMargin = NA, OperatingMargin = NA, ROA = NA, ROE = NA,
#'     Revenue = NA, RevenueShare = NA, TrailingPE = NA, EV = NA,
#'     DivPaid = NA, NetIncome = NA, FreeCashFlow = NA
#'   )
#'   parse_er <- NA
#'   for (i in 1:nrow(xmatch)) {
#'     print(paste0(xmatch[i, 'Code'], ' ', i, ' out of ', nrow(xmatch)))
#'     url <- paste0(
#'       'https://eodhd.com/api/fundamentals/',
#'       xmatch[i, 'Code'], '.', xmatch[i, 'Exchange'],
#'       '?api_token=661ec0af0310e9.32152732&fmt=json'
#'     )
#'     r <- GET(url)
#'     json <- try(parse_json(r))
#'     if ('try-error' %in% class(json)) {
#'       parse_er <- c(parse_er, xmatch$Code[i])
#'       next
#'     }
#'     is <- json$Financials$Income_Statement$yearly[[1]]
#'     df_i <- data.frame(
#'       MarketCap = check_null(json$Highlights, 'MarketCapitilization'),
#'       EBITDA = check_null(json$Highlights, 'EBIDTA'),
#'       DividendShare = check_null(json$Highlights, 'DividendShare'),
#'       EarningsShare = check_null(json$Highlights, 'EarningsShare'),
#'       ProfitMargin = check_null(json$Highlights, 'ProfitMargin'),
#'       OperatingMargin = check_null(json$Highlights, 'OperatingMargin'),
#'       ROA = check_null(json$Highlights, 'ReturnOnAssetsTTM'),
#'       ROE = check_null(json$Highlights, 'ReturnOnEquityTTM'),
#'       Revenue = check_null(json$Highlights, 'RevanueTTM'),
#'       RevenueShare = check_null(json$Highlights, 'RevenueShareTTM'),
#'       TrailingPE = check_null(json$Valuation, 'TrailingPE'),
#'       EV = check_null(json$Valuation, 'EnterpriseValue'),
#'       DivPaid = check_null(is, 'DivPaid'),
#'       NetIncome = check_null(is, 'netIncome'),
#'       FreeCashFlow = check_null(is, 'freeCashFlow')
#'     )
#'     df <- rbind(df, df_i)
#'   }
#' }
#'
#'
#' #' @export
#' clean_us_exchange <- function(df) {
#'   us_ex <- c('US', 'OTC', 'NYSE', 'NYSE ARCA', 'NMFQS', 'PINK', 'BATS',
#'              'NASDAQ', 'OTCQX', 'NYSE MKT', 'OTCMKTS', 'OTCQB', 'OTCGREY',
#'              'OTCCE', 'OTCBB', 'AMEX')
#'   df$Exchange[df$Exchange %in% us_ex] <- 'US'
#'   return(df)
#' }
