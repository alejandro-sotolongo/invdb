#' Database Object
#' 
#' @description
#' Database connects to various data providers, e.g., FactSet, Black Diamond,
#'   SEC EDGAR, etc, to pull data and organize in tables files (arrow, parquet)
#'   in AWS S3.
#' @export
Database <- R6::R6Class(
  'Database',
  public = list(
    
    #' @field msl table: master security list
    msl = NULL,
    
    #' @field geo table: country number and geography
    geo = NULL,

    #' @field api_keys optional list containing api_keys
    api_keys = NULL,
    
    #' @field bucket `S3FileSystem` object from `arrow`
    bucket = NULL,
    
    #' @field macro list with tables for R3000 and ACWI model from Piper Sandler
    macro = NULL,
    
    #' @field ret list with daily and monthly return tables 
    ret = NULL,

    #' @field fina table with latest financial metrics for companies
    fina = NULL,
    
    #' @description Create a Database
    #' @param msl table: master security list
    #' @param geo table: country number and geography
    #' @param bucket `S3FileSystem` object from `arrow`
    #' @param api_keys list of api_keys
    #' @param api_file `.RData` file to load api_key list
    #' @details
        #' All fields are optional, except the api keys must be either entered
        #' as a list through the api_keys parameter or a file path to load the 
        #' api keys via api_file. The tables are data.frames or tibbles stored 
        #' as arrow or parquet files in S3, accessed through the bucket 
        #' S3FileSystem.
    initialize = function(
      msl = NULL,
      geo = NULL,
      bucket = NULL,
      api_keys = NULL,
      api_file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData')
    {
      if (is.null(api_keys)) {
        if (file.exists(api_file)) {
          load(api_file)
          if (exists('api_keys')) {
            self$api_keys <- api_keys
          } else {
            stop('api keys list is not named api_keys')
          }
        } else {
          if (is.null(api_keys)) {
            stop('must either supply api_keys or api_file location')
          } else {
            self$api_keys <- api_keys
          }
        }
      }
      self$check_api_keys()
      bucket <- arrow::s3_bucket(
        'dtc-inv',
        access_key = api_keys$s3$access_key,
        secret_key = api_keys$s3$secret_key
      )
      self$bucket <- bucket
      self$msl <- msl
      self$check_msl()
      self$geo <- geo
      self$check_geo()
      self$bucket <- bucket
    },

    # tables -----

    #' @description save master security list in S3
    #' @param msl master security list
    #' @details
        #' If msl is left as NULL the msl stored in the Database Object as will
        #' be written. If a data.frame or tibble is entered then that will
        #' table will be saved to S3. Note there is only one msl, whatever is 
        #' saved will overwrite the table in S3.
    write_msl = function(msl = NULL) {
      if (is.null(msl)) {
        msl <- self$msl
      }
      if (is.null(msl)) {
        stop('msl is missing')
      }
      if (!'data.frame' %in% class(msl)) {
        stop(paste0('msl is wrong structure ', class(msl)))
      }
      self$check_bucket()
      write_parquet(msl, self$bucket$path('tables/msl.parquet'))
    },

    #' @description Read msl from S3
    read_msl = function() {
      self$check_bucket()
      self$msl <- read_parquet(self$bucket$path('tables/msl.parquet'))
    },

    #' @description Read msl from Excel
    #' @param path file path where workbook is stored
    read_msl_xl = function(
      path = 'N:/Investment Team/DATABASES/MDB/Tables/msl.xlsx') {
      self$msl <- readxl::read_excel(
        path = path,
        sheet = 'msl',
        col_types = c('text', 'text', 'numeric', 'numeric',
                      rep('text', 13), 'numeric')
      )
    },

    #' @description Write geography table
    #' @param geo geography table
    #' @details
    #' If geo is left as NULL the geo stored in the Database Object as will
    #' be written. If a data.frame or tibble is entered then that will
    #' table will be saved to S3. Note there is only one geography table, 
    #' whatever is saved will overwrite the table in S3.
    write_geo = function(geo = NULL) {
      if (is.null(geo)) {
        geo <- self$geo
      }
      if (is.null(geo)) {
        stop('geo is missing')
      }
      if (!'data.frame' %in% class(geo)) {
        stop(paste0('geo is wrong structure ', class(geo)))
      }
      self$check_bucket()
      write_parquet(geo, self$bucket$path('tables/geo.parquet'))
    },

    #' @description Read geography table from S3
    read_geo = function() {
      self$check_bucket()
      self$geo <- read_parquet(self$bucket$path('tables/geo.parquet'))
    },

    #' @description Read geography table from Excel
    #' @param path file path where workbook is stored
    read_geo_xl = function(
      path = 'N:/Investment Team/DATABASES/MDB/Tables/geo.xlsx') {
      self$geo <- readxl::read_excel(
        path = path,
        col_types = c('numeric', rep('text', 4))
      )
    },

    #' @description Read macro workbooks from S3
    read_macro = function() {
      self$check_bucket()
      r3 <- read_parquet(self$bucket$path('macro/macro_r3.parquet'))
      acwi <- read_parquet(self$bucket$path('macro/macro_acwi.parquet'))
      self$macro <- list(r3 = r3, acwi = acwi)
    },

    # checks ----

    #' @description Check if bucket is not NULL
    check_bucket = function() {
      if (is.null(self$bucket)) {
        stop('bucket is missing')
      }
    },

    #' @description
        #' Check if MSL is loaded and has unique DTCNames (key id).
        #'   Will try from S3 if NULL.
    check_msl = function() {
      if (is.null(self$msl)) {
        x <- try(self$read_msl())
        if ('data.frame' %in% class(x)) {
          if (sum(duplicated(x$DTCName))) {
            warning('duplicated DTCNames found')
          }
          return('pass')
        } else {
          stop('msl is missing and could not auto load')
        }
      } else {
        if ('data.frame' %in% class(self$msl)) {
          return('pass')
          if (sum(duplicated(x$DTCName))) {
            warning('duplicated DTCNames found')
          }
        } else {
          stop('msl is not a data.frame')
        }
      }
    },

    #' @description
        #' Check if geography table is not NULL. Will try and read from S3 first.
        #' Will also check if it's a data.frame.
    check_geo = function() {
      if (is.null(self$geo)) {
        x <- try(self$read_geo())
        if ('data.frame' %in% class(x)) {
          return('pass')
        } else {
          stop('geo is missing and could not auto load')
        }
      } else {
        if ('data.frame' %in% class(self$geo)) {
          return('pass')
        } else {
          stop('geo is not a data.frame')
        }
      }
    },

    #' @description
        #' Check if api_keys are properly structured as named list. Will need
        #' to update if more keys are added.
    check_api_keys = function() {
      if (is.null(self$api_keys)) {
        stop('api keys are missing')
      }
      nm_check <- c('t_api', 'bd_key', 's3') %in% names(self$api_keys)
      if (!all(nm_check)) {
        stop('names of api_keys list not properly specified')
      }
    },

    # holdings ----

    #' @description
        #' Pull holdings of portfolio, current sources include Black Diamon,
        #' SEC EDGAR, and Excel.
    #' @param dtc_name key id, DTCName column in master security list
    #' @param as_of optional date for when the holdings are pulled as of, see
    #'   details
    #' @param user_email default is Alejandro's, SEC requires email in API
    #' @param xl_df if updating from Excel, data.frame to pass through
    #' @param return_df data will be saved in S3, if set to TRUE a data.frame
    #'   will also be returned
    #' @details The portfolio (or mutual fund, CTF, SMA, ETF, etc) will need to
    #'   be in the MSL. For existing files, new rows will
    #'   be  added for holdings as of the new date. For SEC sources the most
    #'   recent filings will be pulled. For Black Diamond the date is part of
    #'   the API to pull holdings as of a specific date. If left NULL date will
    #'   default to the last trading day.
    update_holding = function(dtc_name, as_of = NULL, user_email = NULL,
                              xl_df = NULL, return_df = FALSE) {
      if (is.null(as_of)) {
        as_of <- last_us_trading_day()
      }
      if (is.null(user_email)) {
        user_email <- 'alejandro.sotolongo@diversifiedtrust.com'
      }
      self$check_bucket()
      ix <- self$msl$DTCName == dtc_name
      ix[is.na(ix)] <- FALSE
      obs <- self$msl[ix, ]
      if (nrow(obs) == 0) {
        stop('could not find account id in msl')
      }
      if (nrow(obs) > 1) {
        warning('found duplicate account ids in msl, taking first match')
        obs <- obs[1, ]
      }
      hist_df <- read_holdings_file(self$bucket, dtc_name)
      if (is.null(hist_df)) {
        hist_file <- FALSE
      } else {
        hist_file <- TRUE
      }
      if (obs$HoldingsSource == 'SEC') {
        df <- self$update_sec(obs, user_email)
      }
      if (obs$HoldingsSource == 'BD') {
        df <- self$update_bd(obs, self$api_keys, as_of)
      }
      # code for excel inputs
      s3_path <- paste0('holdings/', dtc_name, '.parquet')
      if (hist_file) {
        if (df$returnInfo[1] %in% hist_df$returnInfo) {
          warning(paste0(dtc_name, ' already up to date.'))
          if (return_df) {
            return(df)
          } else {
            return()
          }
        } else {
          new_df <- rob_rbind(hist_df, df)
          write_parquet(new_df, self$bucket$path(s3_path))
        }
      } else {
        warning(paste0(dtc_name, ' not found, creating new file'))
        write_parquet(df, self$bucket$path(s3_path))
      }
      if (return_df) {
        return(df)
      }
    },

    #' @description
        #' Update CTF holdings from Black Diamond
    #' @param update_bd boolean to update underlying holdings first.
    #' @details
        #' The SMA holdings in CTF accounts are updated bi-monthly. As a 
        #' work around the SMAs are read as their own accounts seperately to
        #' get a more recent estimate. If update_bd is set to FALSE the latest
        #' values will be used (last time they were updated). Set to TRUE to
        #' force and update of the underlying SMAs to the last trading day.
    update_all_ctf_holdings = function(update_bd = FALSE) {
      if (update_bd) {
        bd <- subset_df(self$msl, 'HoldingsSource', 'BD')
        for (i in 1:nrow(bd)) {
          self$update_holding(bd$DTCName[i])
        }
      }
      self$update_ctf_holdings('US Active Equity')
      self$update_ctf_holdings('US Core Equity')
    },

    #' @description Update CTF holdings
    #' @param dtc_name DTCName in msl (key id)
    #' @param add_to_existing boolean to add new holdings to old holdings as
    #' a time-series of holdings. If set to FALSE the file with old holdings 
    #' will be overwritten and only new holdings will exist.
    #' @details CAREFUL with add_to_existing. Set to TRUE to continue time-series
    #' of holdings. If you need to overwrite file with just new holdings, set
    #' to FALSE.
    update_ctf_holdings = function(dtc_name, add_to_existing = TRUE) {
      df <- self$update_holding(dtc_name, return_df = TRUE)
      mdf <- merge_msl(df, self$msl)
      bd <- subset_df(mdf$match, 'HoldingsSource', 'BD')
      for (i in 1:nrow(bd)) {
        x <- read_holdings_file(self$bucket, bd$DTCName[i], TRUE)
        mdf$all$emv[mdf$all$DTCName == bd$DTCName[i]] <- sum(x$emv)
      }
      s3_name <- paste0('holdings/', dtc_name, '.parquet')
      df <- mdf$all[, 1:9]
      if (add_to_existing) {
        old_df <- read_parquet(self$bucket$path(s3_name))
        df <- rob_rbind(old_df, df)
      }
      write_parquet(df, self$bucket$path(s3_name)
      )
    },

    #' @description Helper function to update holdings from SEC EDGAR filings
    #' @param obs row of msl with portfolio meta data
    #' @param user_email SEC requires an email address when scraping data
    #' @return data.frame of holdings, does not save to S3
    update_sec = function(obs, user_email) {
      df <- download_sec_nport(obs$LongCIK, obs$ShortCIK, user_email)
      return(df)
    },

    #' @description Helper function to update holdings from Black Diamond
    #' @param obs row of msl with portfolio meta data
    #' @param api_keys description
    #' @param as_of desired date of when to pull holdings from
    #' @return data.frame of holdings, does not save to S3
    #' @details
        #' IMPORTANT: needs to be run locally, api keys for black diamond are
        #' only stored in DTC servers, not AWS.
    update_bd = function(obs, api_keys, as_of) {
      df <- download_bd(obs$BDAccountID, api_keys, as_of)
      return(df)
    },

    #' @description Update all portfolios holdings (except CTFs, see 
    #'   update_ctf_holdings) from Black Diamond and SEC
    #' @param user_email will default to Alejandro's if left NULL, SEC requires
    #' @param as_of for Black Diamond date for when to pull as of, will 
    #'   default to last trading day if letf NULL
    #' @return saves updated holdings in S3
    #' @details
        #' The portfolios to be updated are filtered from the MSL that have
        #' holdings sources of BD or SEC. CTFs are updated with a seperate
        #' process to work around the bi-monthly SMA update within the CTF.
    update_all_port = function(user_email = NULL, as_of = NULL) {
      if (is.null(user_email)) user_email <- 'asotolongo@diversifiedtrust.com'
      ix <- self$msl$HoldingsSource == 'SEC'
      ix[is.na(ix)] <- FALSE
      sec <- self$msl[ix, ]
      if (nrow(sec) == 0) {
        warning('no SEC files found to update')
      } else {
        print('updating SEC')
        for (i in 1:nrow(sec)) {
          print(paste0('updating ', sec$DTCName[i], ' ', i, ' out of ',
                       nrow(sec)))
          self$update_holding(sec$DTCName[i], as_of, user_email)
        }
      }
      ix <- self$msl$HoldingsSource == 'BD'
      ix[is.na(ix)] <- FALSE
      bd <- self$msl[ix, ]
      if (nrow(bd) == 0) {
        warning('no BD files found to update')
      } else {
        print('updating BD')
        for (i in 1:nrow(bd)) {
          print(paste0('updating ', bd$DTCName[i], ' ', i, ' out of ',
                       nrow(bd)))
          self$update_holding(bd$DTCName[i], as_of, user_email)
        }
      }
    },

    #' @description
        #' Subset holdings data.frame for only the most recent holdings
    #' @details
        #' Holdings data.frames will have a time-series containing the history
        #' of holdings, this function will return all of the holdings on the 
        #' most recent date stored in the data.frame.
    get_curr_holdings = function() {
      ix <- self$msl$Layer == 2
      ix[is.na(ix)] <- FALSE
      lay_2 <- self$msl[ix, ]
      df <- data.frame()
      all_files <- self$bucket$ls('holdings/')
      for (i in 1:nrow(lay_2)) {
        file_nm <- paste0('holdings/', lay_2$DTCName[i], '.parquet')
        print(paste0('reading ', file_nm))
        if (file_nm %in% all_files) {
          df_i <- read_parquet(self$bucket$path(file_nm))
          df_i$ParentName <- lay_2$DTCName[i]
          df <- rob_rbind(df, df_i)
        } else {
          warning(paste0(file_nm, ' not found'))
        }
      }
      is_dup <- duplicated(df$cusip, incomparables = c(NA, "000000000")) |
        duplicated(df$ticker, incomparables = NA) |
        duplicated(df$isin, incomparables = NA)
      res <- list(
        all = df,
        uniq = df[!is_dup, ]
      )
      return(res)
    },

    # returns ----

    #' @description Read in all retuns to populate ret field
    #' @return db$ret will now be populated with a list containing two xts
    #'   objects: $d daily returns and $m monthly returns
    read_all_ret = function() {
      d <- read_feather(self$bucket$path('returns/daily/factset.arrow'))
      d <- df_to_xts(d)
      mf <- read_feather(self$bucket$path('returns/daily/mutual-fund.arrow'))
      mf <- df_to_xts(mf)
      ovlp <- colnames(mf) %in% colnames(d)
      if (any(ovlp)) {
        warning(paste0(colnames(mf)[ovlp]), ' found in daily returns')
        mf <- mf[, !ovlp]
      }
      d <- xts_cbind(d, mf)
      ret <- list()
      ret$d <- d
      ret$m <- xts()
      self$ret <- ret
    },
    
    # update_all_tiingo = function(start_date = NULL, end_date = NULL) {
    #   msl <- self$msl
    #   ticker_vec <- msl$ReturnCol[msl$ReturnSource == 'tiingo']
    #   ticker_vec <- na.omit(ticker_vec)
    #   ticker_vec <- unique(ticker_vec)
    #   if (is.null(start_date)) {
    #     start_date <- '1970-01-01'
    #   }
    #   price <- download_tiingo_tickers(ticker_vec, self$api_keys$t_api,
    #                                    start_date, end_date)
    #   price_xts <- xts(price[, -1], price[[1]])
    #   ret <- price_to_ret(price_xts)
    #   ret_df <- xts_to_dataframe(ret)
    #   write_parquet(ret_df, self$bucket$path('return/tiingo.parquet'))
    # },

    # update_tiingo_daily = function(date_start = NULL, date_end = NULL) {
    #   msl <- self$msl
    #   ticker_vec <- msl$ReturnCol[msl$ReturnSource == 'tiingo']
    #   ticker_vec <- na.omit(ticker_vec)
    #   ticker_vec <- unique(ticker_vec)
    #   hist_ret <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
    #   add_ticker <- ticker_vec[!ticker_vec %in% colnames(hist_ret)]
    #   if (is.null(date_end)) {
    #     date_end <- last_us_trading_day()
    #   }
    #   if (is.null(date_start)) {
    #     td <- us_trading_days(date_end - 6, date_end)
    #     date_start <- td[1]
    #   }
    #   price <- download_tiingo_tickers(ticker_vec, self$api_keys$t_api,
    #                                    date_start = date_start,
    #                                    date_end = date_end)
    #   price_xts <- xts(price[, -1], price[[1]])
    #   hist_ret <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
    #   hist_ret <- xts(hist_ret[, -1], hist_ret[[1]])
    #   add_price <- download_tiingo_tickers(add_ticker, self$api_keys$t_api,
    #                                        as.Date('1970-01-01'), date_end,
    #                                        out_ret = TRUE, out_xts = TRUE)
    #   if (!is.null(add_price)) {
    #     hist_ret <- xts_cbind(hist_ret, add_price)
    #   }
    #   ret <- price_to_ret(price_xts)
    #   combo_ret <- xts_rbind(hist_ret, ret)
    #   combo_ret_df <- xts_to_dataframe(combo_ret)
    #   write_parquet(combo_ret_df, self$bucket$path('returns/daily/tiingo.parquet'))
    # },


    #' @description Get factset request ids from the msl
    #' @param max_iter_by number to sequence large number of ids
    #' @details
    #'   Large amounts of ids need to broken up, some factset API requests only
    #'   allow 50 ids at a time. Request ids first look for ISIN, then CUSIP, 
    #'   SEDOL, LEI, and Identifier in order. The function returns a list with
    #'   ids and iter for global prices and fi_ids and fi_iter for the formula 
    #'   API (mutual fund returns). The iter is the sequence of large amounts
    #'   of ids to be broken up into smaller pieces. For small numbers, e.g., 
    #'   10 ids will contain and iter of c(1, 10).
    filter_fs_ids = function(max_iter_by = 50) {
      fs <- subset_df(self$msl, 'ReturnSource', 'factset')
      fmf <- subset_df(fs, 'SecType', 'Mutual Fund')
      fs <- fs[!fs$Ticker %in% fmf$Ticker, ]
      ids <- fs$ISIN
      ids[is.na(ids)] <- fs$CUSIP[is.na(ids)]
      ids[is.na(ids)] <- fs$SEDOL[is.na(ids)]
      ids[is.na(ids)] <- fs$LEI[is.na(ids)]
      ids[is.na(ids)] <- fs$Identifier[is.na(ids)]
      ids <- gsub(' ', '', ids)
      ids <- na.omit(ids)
      iter <- get_iter(ids)
      fi_ids <- fmf$Identifier
      fi_iter <- get_iter(fi_ids)
      res <- list()
      res$ids <- ids
      res$iter <- iter
      res$fi_ids <- fi_ids
      res$fi_iter <- fi_iter
      return(res)
    },

    #' @description
        #' Run after each quarter to add most recent fundamental data point 
        #' (e.g., P/E, ROE) to S3 table.
    #' @details
        #' The files are organized by metric. For example the P/E file will
        #' contain a time-series of P/Es for each company. This function reads all
        #' the metrics and combines the latest values for each company into one
        #' table.
    update_fs_fina_most_recent = function() {
      dat <- list()
      dat$pe <- read_feather(self$bucket$path('co-data/arrow/PE.arrow'))
      dat$pb <- read_feather(self$bucket$path('co-data/arrow/PB.arrow'))
      dat$pfcf <- read_feather(self$bucket$path('co-data/arrow/PFCF.arrow'))
      dat$dy <- read_feather(self$bucket$path('co-data/arrow/DY.arrow'))
      dat$roe <- read_feather(self$bucket$path('co-data/arrow/ROE.arrow'))
      dat$mcap <- read_feather(self$bucket$path('co-data/arrow/MCAP.arrow'))
      dat <- lapply(dat, function(x) {xts(x[, -1], as.Date(x[[1]]))})
      dat <- lapply(dat, fill_na_price)
      all_co <- unlist(unique(lapply(dat, colnames)))
      fina <- matrix(nrow = length(all_co), ncol = length(dat))
      for (i in 1:length(dat)) {
        co_match <- match(colnames(dat[[i]]), all_co)
        fina[co_match, i] <- dat[[i]][nrow(dat[[i]]), ]
      }
      # will need to adjust colnames below if fields are changed
      colnames(fina) <- c('PE', 'PB', 'PFCF', 'DY', 'ROE', 'MCAP')
      fina_df <- data.frame(DTCName = all_co, fina)
      write_feather(fina_df, self$bucket$path('co-data/arrow/latest-fina.arrow'))
      as_of <- sapply(dat, function(x) {zoo::index(x)[nrow(x)]})
      # will need to adust as_of_df below if fields are changed
      as_of_df <- data.frame(
        Field = c('PE', 'PB', 'PFCF', 'DY', 'ROE', 'MCAP'),
        AsOf = as.Date(as_of)
      )
      write_feather(as_of_df, self$bucket$path('co-data/arrow/fina-as-of.arrow'))
    },

    #' @description
    #' Run after each quarter to add most recent fundamental data point 
    #' (e.g., P/E, ROE) to S3 table.
    #' @param yrs_back integer for how many years back to pull most recent data
    #' @details
    #' Currently set up for P/E, P/B, P/FCF, DY, ROE, and Mkt Cap. The data
    #' are organized by the metric. The P/E table will contain a time-series
    #' of the P/E for each stock. 
    update_fs_fina_quarterly = function(yrs_back = 1,
      dtype = c('PE', 'PB', 'PFCF', 'DY', 'ROE', 'MCAP')) {
      # TO-DO read old file and and new row
      if (dtype == 'PE') {
        formulas <- paste0('FG_PE(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'PB') {
        formulas <- paste0('FG_PBK(QTR_R,-', yrs_back, 'AY,NOW,CQ')
      } else if (dtype == 'PFCF') {
        formulas <- paste0('FG_CFLOW_FREE_EQ_PS(-', yrs_back, 'AY,NOW,CQ,USD)')
      } else if (dtype == 'DY') {
        formulas <- paste0('FG_DIV_YLD(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'ROE') {
        formulas <- paste0('FG_ROE(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'MCAP') {
        formulas <- paste0('FF_MKT_VAL(ANN_R,-', yrs_back, 'AY,NOW,CQ,,USD)')
      } else {
        stop("dtype must be 'PE', 'PB', 'PFCF', 'DY', 'ROE', or 'MCAP'")
      }
      res <- self$filter_fs_ids()
      ids <- res$ids
      iter <- res$iter
      fund_list <- list()
      for (i in 1:(length(iter)-1)) {
        xids <- ids[iter[i]:iter[i+1]]
        json <- download_fs(
          api_keys = self$api_keys,
          ids = xids,
          formulas = formulas,
          type = 'cs'
        )
        dat <- json$data
        res <- lapply(dat, '[[', 'result')
        dt <- sapply(res, '[[', 'dates')
        dt <- sort(unique(unlist(dt)))
        val_mat <- matrix(nrow = length(dt), ncol = length(res))
        for (j in 1:length(res)) {
          if (all(c('dates', 'values') %in% names(res[[j]]))) {
            miss_date <- sapply(res[[j]]$dates, is.null)
            res[[j]]$values[sapply(res[[j]]$values, is.null)] <- NA
            xdt <- unlist(res[[j]]$dates)
            date_match <- match(xdt, dt)
            xval <- unlist(res[[j]]$values)
            xval <- xval[!miss_date]
          } else {
            warning(paste0(dat[[j]]$requestId, ' not properly structured'))
            date_match <- 1:length(dt)
            xval <- rep(NA, length(dt))
          }
          val_mat[date_match, j] <- xval
        }
        if (any(is.na(xids))) {
          miss_ids <- which(is.na(xids))
          colnames(val_mat) <- self$msl$DTCName[iter[i]:iter[i+1]][-miss_ids]
        } else {
          colnames(val_mat) <- self$msl$DTCName[iter[i]:iter[i+1]]
        }
        fund_list$val[[i]] <- val_mat
        fund_list$dt[[i]] <- dt
        print(iter[i])
      }
      udt <- unique(unlist(fund_list$dt))
      m_vec <- sapply(fund_list$val, ncol)
      cum_m_vec <- cumsum(m_vec)
      fund_mat <- matrix(nrow = length(udt), ncol = sum(m_vec))
      date_match <- match(fund_list$dt[[1]], udt)
      fund_mat[date_match, 1:m_vec[1]] <- fund_list$val[[1]]
      for (i in 2:length(cum_m_vec)) {
        dm <- match(fund_list$dt[[i]], udt)
        fund_mat[dm, (cum_m_vec[i-1]+1):cum_m_vec[i]] <- fund_list$val[[i]]
      }
      fund_df <- data.frame(Date = month_end(udt), fund_mat)
      colnames(fund_df)[-1] <- unlist(sapply(fund_list$val, colnames))
      write_feather(
        fund_df,
        self$bucket$path(paste0('co-data/arrow/', dtype, '.arrow'))
      )
      write_parquet(
        fund_df,
        self$bucket$path(paste0('co-data/parquet', dtype, '.parquet'))
      )
    },

    
    #' @description
        #' Update mutual fund returns with Factset formula API
    #' @param days_back integer for how many trading days to pull history for, 
    #'   default is zero to pull only latest trading day
    #' @details
    #'   Will add new returns to existing time-series stored in S3. New 
    #'   time-series that are not in the old file will have NAs for older returns
    #'   depending on the history available and the days_back input. For 
    #'   existing time-series, any overlap in the new and old returns will be
    #'   overwritten with the new returns.
    update_fs_mf_ret_daily = function(days_back = 0) {
      old_ret <- read_feather(self$bucket$path('returns/daily/mutual-fund.arrow'))
      old_ret <- df_to_xts(old_ret)
      res <- self$filter_fs_ids()
      iter <- res$fi_iter
      ids <- res$fi_ids
      ret_df <- data.frame()
      formulas <- paste0('P_TOTAL_RETURNC(-', days_back, 'D,NOW,D,USD)')
      for (i in 1:(length(iter)-1)) {
        xids <- ids[iter[i]:iter[i+1]]
        json <- download_fs(
          api_keys = self$api_keys,
          ids = xids,
          formulas = formulas,
          type = 'ts'
        )
        dat <- json$data
        res <- lapply(dat, '[[', formulas)
        res <- list_replace_null(res)
        res <- unlist(res)
        dt <- sapply(dat, '[[', 'date')
        req_id <- sapply(dat, '[[', 'requestId')
        i_df <- data.frame(requestId = req_id, date = dt, value = res)
        ret_df <- rob_rbind(ret_df, i_df)
      }
      ret_df$value <- ret_df$value / 100
      ix <- request_id_match_msl(ret_df$requestId, self$msl)
      ret_df$DTCName <- self$msl$DTCName[ix]
      is_dup <- duplicated(paste0(ret_df$DTCName, ret_df$date))
      ret_df <- ret_df[!is_dup, ]
      wdf <- pivot_wider(ret_df, id_cols = date, values_from = value, 
                         names_from = DTCName)
      wdf <- df_to_xts(wdf)
      combo <- xts_rbind(old_ret, wdf)
      combo <- xts_to_dataframe(combo)
      write_feather(combo, self$bucket$path('returns/daily/mutual-fund.arrow'))
    },

    #' @description
        #' Update returns for all investments traded on an exchange 
        #' (e.g., stocks, ETFs) with Factset Global Prices API
    #' @param ids optional, can pass through string of ids, otherwise will
    #'   pull from the master security list
    #' @param date_start optional string 'YYYY-MM-DD' to represent the first
    #'   date in the time-series
    #' @param date_end optional string 'YYYY-MM-DD' to represent the last date
    #'   in the time-series.
    #' @details
    #'   The default for dates is to pull the most recent trading days return.
    #'   The routine is run overnight to add a new return to the existing 
    #'   table of returns. To add new returns specify ids and set the date_start
    #'   farther back (~5 years) to create a history.
    update_fs_ret_daily = function(ids = NULL, date_start = NULL,
                                   date_end = NULL) {
      old_ret <- read_feather(self$bucket$path('returns/daily/factset.arrow'))
      old_xts <- df_to_xts(old_ret)
      if (is.null(ids)) {
        res <- self$filter_fs_ids(50)
      } else {
        res <- list()
        if (length(ids) > 50) {
          iter <- seq(1, length(ids), 49)
          if (iter[length(iter)] < length(ids)) {
            iter <- c(iter, length(ids))
          }
        } else {
          iter <- c(1, length(ids))
        }
        res$ids <- ids
        res$iter <- iter
      }
      if (is.null(date_start)) {
        date_start <- Sys.Date() - 1
      }
      if (is.null(date_end)) {
        date_end <- Sys.Date() - 1
      }
      df <- data.frame()
      for (i in 1:(length(res$iter)-1)) {
        json <- download_fs_gp(
          api_keys = self$api_keys,
          ids = res$ids[res$iter[i]:res$iter[i+1]],
          date_start = date_start,
          date_end = date_end,
          freq = 'D'
        )
        df <- rob_rbind(df, flatten_fs_gp(json))
        print(res$iter[i])
      }
      ix <- request_id_match_msl(df$requestId, self$msl)
      df$DTCName <- self$msl$DTCName[ix]
      df$totalReturn <- df$totalReturn / 100
      is_dup <- duplicated(paste0(df$DTCName, df$date))
      df <- df[!is_dup, ]
      wdf <- pivot_wider(df, id_cols = date, values_from = totalReturn,
                         names_from = DTCName)
      wxts <- df_to_xts(wdf)
      combo <- xts_rbind(wxts, old_xts)
      df_out <- xts_to_dataframe(combo)
      write_feather(df_out, self$bucket$path('returns/daily/factset.arrow'))
    },

    
    update_ctf_monthly = function() {
      ctf <- subset_df(self$msl, 'ReturnSource', 'ctf_d')
      rl <- list()
      for (i in 1:nrow(ctf)) {
        id <- 
      }
    }
    
    # update_ctf_daily = function() {
    #   ix <- self$msl$ReturnSource == 'ctf_d'
    #   ix[is.na(ix)] <- FALSE
    #   factset <- self$msl[ix, ]
    #   res <- list()
    #   for (i in 1:nrow(factset)) {
    #     id <- paste0("CLIENT:/PA_SOURCED_RETURNS/", factset$ISIN[i])
    #     res[[i]] <- download_fs_ret(id, self$api_keys)
    #   }
    #   res_ret <- lapply(res, '[[', 'ret')
    #   is_miss <- function(x) {
    #     if (is.null(nrow(x))) {
    #       return(TRUE)
    #     }
    #     if (nrow(x) == 0) {
    #       return(TRUE)
    #     } else {
    #       return(FALSE)
    #     }
    #   }
    #   miss_ret <- sapply(res_ret, is_miss)
    #   new_ret <- do.call('cbind', res_ret[!miss_ret])
    #   colnames(new_ret) <- factset$ReturnCol[!miss_ret]
    #   hist_ret <- read_parquet(self$bucket$path("returns/daily/ctf_d.parquet"))
    #   hist_ret <- xts(hist_ret[, -1], hist_ret[[1]])
    #   combo <- xts_rbind(hist_ret, new_ret)
    #   combo <- xts_to_dataframe(combo)
    #   write_parquet(combo, self$bucket$path("returns/daily/ctf_d.parquet"))
    # },

    update_pdf_funds = function() {
      wb <- 'N:/Investment Team/CTFs/Private Diversifiers/PDF Workup.xlsx'
      sht <- 'input_mgr_return'
      ret <- read_xts(wb, sht, 1)
      df <- xts_to_dataframe(ret)
      write_parquet(df, self$bucket$path('returns/monthly/pdf.parquet'))
    },

    load_ret = function() {
      # ctf_d <- read_parquet(self$bucket$path('returns/daily/ctf_d.parquet'))
      # tiingo <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
      ret <- list()
      # d <- xts_cbind(
      #   mat_to_xts(ctf_d),
      #   mat_to_xts(tiingo)
      # )
      # ret$d <- d
      xpdf <- read_parquet(self$bucket$path('returns/monthly/pdf.parquet'))
      ret$m <- mat_to_xts(xpdf)
      self$ret <- ret
    },

    # macro ----

    update_macro = function(fpath = NULL,
                            acwi = 'D_MACRO_SELECT_GLOBAL',
                            r3 = 'D_MACRO_SELECT_R',
                            fct_name = c('A', 'B', 'C', 'D', 'E')) {
      if (is.null(fpath)) {
        fpath <- 'N:/Investment Team/DATABASES/MDB/Tables/'
      }
      acwi_df <- read_macro_wb(paste0(fpath, acwi, '.xlsx'), idx = 'MSCI ACWI',
                               fct_name)
      r3_df <- read_macro_wb(paste0(fpath, r3, '.xlsx'), idx = 'Russell 3000',
                             fct_name)
      self$check_bucket()
      self$macro <- list(r3 = r3_df, acwi = acwi_df)
      write_parquet(r3_df, self$bucket$path('macro_r3.parquet'))
      write_parquet(acwi_df, self$bucket$path('macro_acwi.parquet'))
    },

    # reports ----

    missing_from_msl = function() {
      self$check_msl()
      holdings <- self$get_curr_holdings()
      mdf <- merge_msl(holdings$uniq, self$msl)
    },

    check_ctf_holdings = function() {
      self$check_msl()
      us_act <- read_parquet(self$bucket$path('holdings/US Active Equity.parquet'))
      us_cor <- read_parquet(self$bucket$path('holdings/US Core Equity.parquet'))
      intl <- read_parquet(self$bucket$path('holdings/International Equity.parquet'))

    }
  )
)
