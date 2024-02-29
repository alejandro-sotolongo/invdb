Database <- R6::R6Class(
  'Database',
  public = list(
    msl = NULL,
    layer = NULL,
    geo = NULL,
    api_keys = NULL,
    bucket = NULL,
    macro = NULL,

    initialize = function(
      msl = NULL,
      layer = NULL,
      geo = NULL,
      bucket = NULL,
      api_file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData',
      auto_load_bucket = FALSE)
    {
      api_keys <- list()
      load(api_file)
      if (!exists('api_keys')) {
        stop('could not find api_keys from load(api_file)')
      }
      self$api_keys <- api_keys
      self$check_api_keys()
      if (is.null(bucket) & auto_load_bucket) {
        if (is.null(api_keys$s3)) {
          stop('s3 api keys not found for auto_load_bucket')
        }
        bucket <- arrow::s3_bucket(
          'dtc-inv',
          access_key = api_keys$s3$access_key,
          secret_key = api_keys$s3$secret_key
        )
      }
      self$msl <- msl
      self$layer <- layer
      self$api_keys <- api_keys
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
      write_parquet(msl, bucket$path('msl.parquet'))
    },

    read_msl = function() {
      self$check_bucket()
      self$msl <- read_parquet(self$bucket$path('msl.parquet'))
    },

    read_msl_xl = function() {
      self$msl <- readxl::read_excel('N:/Investment Team/DATABASES/MDB/Tables/msl.xlsx',
                                     'msl')
    },

    write_layer = function(layer = NULL) {
      if (is.null(layer)) layer <- self$layer
      if (is.null(layer)) stop('layer is missing')
      if (!'data.frame' %in% class(layer)) {
        stop(paste0('layer is wrong structure ', class(layer)))
      }
      self$check_bucket()
      write_parquet(layer, bucket$path('layer.parquet'))
    },

    read_layer = function() {
      self$check_bucket()
      self$layer <- read_parquet(self$bucket$path('layer.parquet'))
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
      write_parquet(geo, self$bucket$path('geo.parquet'))
    },

    read_geo = function() {
      self$check_bucket()
      self$geo <- read_parquet(self$bucket$path('geo.parquet'))
    },

    read_geo_xl = function() {
      self$geo <- readxl::read_excel('N:/Investment Team/DATABASES/MDB/Tables/geo.xlsx')
    },

    read_macro = function() {
      self$check_bucket()
      r3 <- read_parquet(self$bucket$path('macro_r3.parquet'))
      acwi <- read_parquet(self$bucket$path('macro_acwi.parquet'))
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
        stop('msl is missing')
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

    # updates ----

    update_all_tiingo = function(start_date = NULL, end_date = NULL) {
      self$check_bucket()
      self$check_msl()
      self$check_api_keys()
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
      write_parquet(ret_df, self$bucket$path('tiingo.parquet'))
    },

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
    }
  )
)
