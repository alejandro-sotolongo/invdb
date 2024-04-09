#' @export
Database <- R6::R6Class(
  'Database',
  public = list(
    msl = NULL,
    geo = NULL,
    api_keys = NULL,
    bucket = NULL,
    macro = NULL,
    ret = NULL,

    initialize = function(
      msl = NULL,
      geo = NULL,
      bucket = NULL,
      api_keys = NULL,
      api_file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData',
      pull_ret = TRUE)
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
      if (pull_ret) {
        self$load_ret()
      }
    },

    # tables -----

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

    read_msl = function() {
      self$check_bucket()
      self$msl <- read_parquet(self$bucket$path('tables/msl.parquet'))
    },

    read_msl_xl = function() {
      self$msl <- readxl::read_excel(
        path = 'N:/Investment Team/DATABASES/MDB/Tables/msl.xlsx',
        sheet = 'msl',
        col_types = c('text', 'text', 'numeric', 'numeric',
                      rep('text', 13), 'numeric')
      )
    },

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

    read_geo = function() {
      self$check_bucket()
      self$geo <- read_parquet(self$bucket$path('tables/geo.parquet'))
    },

    read_geo_xl = function() {
      self$geo <- readxl::read_excel(
        path = 'N:/Investment Team/DATABASES/MDB/Tables/geo.xlsx',
        col_types = c('numeric', rep('text', 4))
      )
    },

    read_macro = function() {
      self$check_bucket()
      r3 <- read_parquet(self$bucket$path('macro/macro_r3.parquet'))
      acwi <- read_parquet(self$bucket$path('macro/macro_acwi.parquet'))
      self$macro <- list(r3 = r3, acwi = acwi)
    },

    # checks ----

    check_bucket = function() {
      if (is.null(self$bucket)) {
        stop('bucket is missing')
      }
    },

    check_msl = function() {
      if (is.null(self$msl)) {
        x <- try(self$read_msl())
        if ('data.frame' %in% class(x)) {
          return('pass')
        } else {
          stop('msl is missing and could not auto load')
        }
      } else {
        if ('data.frame' %in% class(self$msl)) {
          return('pass')
        } else {
          stop('msl is not a data.frame')
        }
      }
    },

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

    update_holding = function(dtc_name, as_of = NULL, user_email = NULL,
                              xl_df = NULL, overwrite = FALSE) {
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
          return()
        } else {
          new_df <- rob_rbind(hist_df, df)
          write_parquet(new_df, self$bucket$path(s3_path))
        }
      } else {
        warning(paste0(dtc_name, ' not found, creating new file'))
        write_parquet(df, self$bucket$path(s3_path))
      }
    },

    update_sec = function(obs, user_email) {
      df <- download_sec_nport(obs$LongCIK, obs$ShortCIK, user_email)
      return(df)
    },

    update_bd = function(obs, api_keys, as_of) {
      df <- download_bd(obs$BDAccountID, api_keys, as_of)
      return(df)
    },

    update_all_port = function(user_email = NULL, as_of = NULL, update_csv = FALSE) {
      if (is.null(user_email)) user_email <- 'asotolongo@diversifiedtrust.com'
      ix <- self$msl$HoldingsSource == 'SEC'
      ix[is.na(ix)] <- FALSE
      sec <- self$msl[ix, ]
      if (nrow(sec) == 0) {
        warning('no SEC files found to update')
      } else {
        print('updating SEC')
        for (i in 1:nrow(sec)) {
          print(paste0('updating ', sec$DTCName[i], ' ', i, ' out of ', nrow(sec)))
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
          print(paste0('updating ', bd$DTCName[i], ' ', i, ' out of ', nrow(bd)))
          self$update_holding(bd$DTCName[i], as_of, user_email)
        }
      }
    },

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

    update_factset = function() {
      self$check_msl()
      self$check_bucket()
      self$check_api_keys()
      ids <- na.omit(unique(self$msl$ISIN))
      eps <- download_fs_large_ids(
        self$api_keys,
        ids,
        "FF_EPS(QTR_R,0)"
      )
    },

    # returns ----

    update_all_tiingo = function(start_date = NULL, end_date = NULL) {
      msl <- self$msl
      ticker_vec <- msl$ReturnCol[msl$ReturnSource == 'tiingo']
      ticker_vec <- na.omit(ticker_vec)
      ticker_vec <- unique(ticker_vec)
      if (is.null(start_date)) {
        start_date <- '1970-01-01'
      }
      price <- download_tiingo_tickers(ticker_vec, self$api_keys$t_api,
                                       start_date, end_date)
      price_xts <- xts(price[, -1], price[[1]])
      ret <- price_to_ret(price_xts)
      ret_df <- xts_to_dataframe(ret)
      write_parquet(ret_df, self$bucket$path('return/tiingo.parquet'))
    },

    update_tiingo_daily = function(date_start = NULL, date_end = NULL) {
      msl <- self$msl
      ticker_vec <- msl$ReturnCol[msl$ReturnSource == 'tiingo']
      ticker_vec <- na.omit(ticker_vec)
      ticker_vec <- unique(ticker_vec)
      hist_ret <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
      add_ticker <- ticker_vec[!ticker_vec %in% colnames(hist_ret)]
      if (is.null(date_end)) {
        date_end <- last_us_trading_day()
      }
      if (is.null(date_start)) {
        td <- us_trading_days(date_end - 6, date_end)
        date_start <- td[1]
      }
      price <- download_tiingo_tickers(ticker_vec, self$api_keys$t_api,
                                       date_start = date_start,
                                       date_end = date_end)
      price_xts <- xts(price[, -1], price[[1]])
      hist_ret <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
      hist_ret <- xts(hist_ret[, -1], hist_ret[[1]])
      add_price <- download_tiingo_tickers(add_ticker, self$api_keys$t_api,
                                           as.Date('1970-01-01'), date_end)
      if (!is.null(add_price)) {
        xts_cbind(hist_ret, add_price)
      }
      ret <- price_to_ret(price_xts)
      combo_ret <- xts_rbind(hist_ret, ret)
      combo_ret_df <- xts_to_dataframe(combo_ret)
      write_parquet(combo_ret_df, self$bucket$path('returns/daily/tiingo.parquet'))
    },

    update_ctf_daily = function() {
      ix <- self$msl$ReturnSource == 'factset'
      ix[is.na(ix)] <- FALSE
      factset <- self$msl[ix, ]
      res <- list()
      for (i in 1:nrow(factset)) {
        id <- paste0("CLIENT:/PA_SOURCED_RETURNS/", factset$ISIN[i])
        res[[i]] <- download_fs_ret(id, self$api_keys)
      }
      res_ret <- lapply(res, '[[', 'ret')
      is_miss <- function(x) {
        if (is.null(nrow(x))) {
          return(TRUE)
        }
        if (nrow(x) == 0) {
          return(TRUE)
        } else {
          return(FALSE)
        }
      }
      miss_ret <- sapply(res_ret, is_miss)
      new_ret <- do.call('cbind', res_ret[!miss_ret])
      colnames(new_ret) <- factset$ReturnCol[!miss_ret]
      hist_ret <- read_parquet(self$bucket$path("returns/daily/ctf_d.parquet"))
      hist_ret <- xts(hist_ret[, -1], hist_ret[[1]])
      combo <- xts_rbind(hist_ret, new_ret)
      combo <- xts_to_dataframe(combo)
      write_parquet(combo, self$bucket$path("returns/daily/ctf_d.parquet"))
    },


    update_pdf_funds = function() {
      wb <- 'N:/Investment Team/CTFs/Private Diversifiers/PDF Workup.xlsx'
      sht <- 'input_mgr_return'
      ret <- read_xts(wb, sht, 1)
      df <- xts_to_dataframe(ret)
      write_parquet(df, self$bucket$path('returns/monthly/pdf.parquet'))
    },

    load_ret = function() {
      ctf_d <- read_parquet(self$bucket$path('returns/daily/ctf_d.parquet'))
      tiingo <- read_parquet(self$bucket$path('returns/daily/tiingo.parquet'))
      ret <- list()
      ret$d$ctf <- ctf_d
      ret$d$tiingo <- tiingo
      ret$m$pdf <- read_parquet(self$bucket$path('returns/monthly/pdf.parquet'))
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
