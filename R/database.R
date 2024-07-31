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

    # update_bd: boolean for updating all underling SMAs first
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

    # update CTFs, need to account for SMA values in BD being updated at
    # SMA level but not CTF level
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

    update_sec = function(obs, user_email) {
      df <- download_sec_nport(obs$LongCIK, obs$ShortCIK, user_email)
      return(df)
    },

    update_bd = function(obs, api_keys, as_of) {
      df <- download_bd(obs$BDAccountID, api_keys, as_of)
      return(df)
    },

    update_all_port = function(user_email = NULL, as_of = NULL,
                               update_csv = FALSE) {
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
      ids <- na.omit(unique(self$msl$ISIN))
      eps <- download_fs_large_ids(
        self$api_keys,
        ids,
        "FF_EPS(QTR_R,0)"
      )
    },

    update_etf = function() {
      etf <- read_parquet(self$bucket$path('ETF/etf_dict.parquet'))
      for (i in 1:nrow(etf)) {
        df <- try(download_sec_nport(etf$FundCIK[i], etf$ParentCIK[i], 'als1012@gmail.com'))
        if ('try-error' %in% class(df)) {
          print('could not download')
          next
        }
        write_parquet(df, self$bucket$path(paste0('ETF/nport/', etf$Name[i], '.parquet')))
        print(paste0(etf$Name[i], ' ', i, ' out of ', nrow(etf)))
      }
    },

    # returns ----

    read_all_ret = function() {
      d <- read_feather(self$bucket$path('returns/daily/factset.arrow'))
      d <- df_to_xts(d)
      ret <- list()
      ret$d <- d
      ret$m <- xts()
      self$ret <- ret
    },
    
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
                                           as.Date('1970-01-01'), date_end,
                                           out_ret = TRUE, out_xts = TRUE)
      if (!is.null(add_price)) {
        hist_ret <- xts_cbind(hist_ret, add_price)
      }
      ret <- price_to_ret(price_xts)
      combo_ret <- xts_rbind(hist_ret, ret)
      combo_ret_df <- xts_to_dataframe(combo_ret)
      write_parquet(combo_ret_df, self$bucket$path('returns/daily/tiingo.parquet'))
    },


    filter_fs_ids = function(max_iter_by = 100) {
      fs <- subset_df(self$msl, 'ReturnSource', 'factset')
      ids <- fs$ISIN
      ids[is.na(ids)] <- fs$CUSIP[is.na(ids)]
      ids[is.na(ids)] <- fs$SEDOL[is.na(ids)]
      ids[is.na(ids)] <- fs$LEI[is.na(ids)]
      ids[is.na(ids)] <- fs$Identifier[is.na(ids)]
      ids <- gsub(' ', '', ids)
      ids <- na.omit(ids)
      if (length(ids) > max_iter_by) {
        iter <- iter <- seq(1, length(ids), (max_iter_by-1))
        if (iter[length(iter)] < length(ids)) {
          iter <- c(iter, length(ids))
        }
        ret_list <- list()
      } else {
        mid <- round(length(ids) / 2, 0)
        iter <- seq(1, mid, length(ids))
      }
      res <- list()
      res$ids <- ids
      res$iter <- iter
      return(res)
    },

    # run after each quarter update of financial data from factset,
    # takes latest data point of time-series of financial data for each
    # company and put's into one data.frame
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

    # Update Factset financial data for stock universe each quarter
    # yrs_back = how many years back to pull data
    # will save arrow and parquet files to S3
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


    # updates factset returns every day in overnight routine
    # days_back: how many weekdays days (includes U.S. holidays) to pull,
    #  default is zero, meaning just yesterday's return
    # update_fs_ret_daily = function(days_back = 0) {
    #   old_ret <- read_parquet(self$bucket$path('returns/daily/factset.parquet'))
    #   old_ret <- xts(old_ret[, -1], as.Date(old_ret[[1]]))
    #   res <- self$filter_fs_ids()
    #   iter <- res$iter
    #   ids <- res$ids
    #   ret_list <- list()
    #   for (i in 1:(length(iter)-1)) {
    #     xids <- ids[iter[i]:iter[i+1]]
    #     json <- download_fs(
    #       api_keys = self$api_keys,
    #       ids = xids,
    #       formulas = paste0('FG_TOTAL_RETURNC(-', days_back, 'D,NOW,D,USD)'),
    #       type = 'cs'
    #     )
    #     dat <- json$data
    #     res <- lapply(dat, '[[', 'result')
    #     dt <- sapply(res, '[[', 'dates')
    #     dt <- sort(unique(unlist(dt)))
    #     val_mat <- matrix(nrow = length(dt), ncol = length(res))
    #     for (j in 1:length(res)) {
    #       if (all(c('dates', 'values') %in% names(res[[j]]))) {
    #         xdt <- unlist(res[[j]]$dates)
    #         date_match <- match(xdt, dt)
    #         res[[j]]$values[sapply(res[[j]]$values, is.null)] <- NA
    #         xval <- unlist(res[[j]]$values)
    #       } else {
    #         warning(paste0(dat[[j]]$requestId, ' not properly structured'))
    #         date_match <- 1:length(dt)
    #         xval <- rep(NA, length(dt))
    #       }
    #       val_mat[date_match, j] <- xval
    #     }
    #     if (any(is.na(xids))) {
    #       miss_ids <- which(is.na(xids))
    #       colnames(val_mat) <- self$msl$DTCName[iter[i]:iter[i+1]][-miss_ids]
    #     } else {
    #       colnames(val_mat) <- self$msl$DTCName[iter[i]:iter[i+1]]
    #     }
    #     ret_list$ret[[i]] <- val_mat
    #     ret_list$dt[[i]] <- dt
    #     print(iter[i])
    #   }
    #   ret <- do.call('cbind', ret_list$ret)
    #   ret <- ret / 100
    #   dt <- unique(unlist(ret_list$dt))
    #   if (length(dt) != nrow(ret)) {
    #     warning("dates and returns don't match, return list")
    #     return(ret_list)
    #   }
    #   ret <- xts(ret, as.Date(dt))
    #   ret_update <- xts_rbind(old_ret, ret, overwrite = TRUE)
    #   ret_df <- xts_to_dataframe(ret_update)
    #   write_parquet(ret_df, self$bucket$path('returns/daily/factset.parquet'))
    #   write_arrow(ret_df, self$bucket$path('returns/daily/factset.arrow'))
    # },


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


    update_ctf_daily = function() {
      ix <- self$msl$ReturnSource == 'ctf_d'
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
