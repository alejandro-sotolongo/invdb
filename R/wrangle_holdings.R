#' @export
port_from_holdings <- function(db, dtc_name = NULL, df = NULL, 
                               expand_all = FALSE, nm = 'Port',
                               incpt = c('first_ret', 'first_wgt'),
                               reb_freq = c('BH', 'D', 'M', 'Q', 'A')) {
  incpt <- incpt[1]
  if (!is.null(dtc_name)) {
    df <- read_holdings_file(db$bucket, dtc_name)
  }
  if (is.null(df)) {
    error('must either supply a valid dtc_name or data.frame as df')
  }
  mdf <- merge_msl(df, db$msl)
  res <- match_ret_df(mdf, db$ret)
  wgt <- df_to_wgt(mdf$match)
  if (incpt == 'first_ret') {
    date_start <- first_comm_start(res$asset_ret)
    asset_ret <- cut_time(res$asset_ret, date_start)
  } else {
    date_start <- index(wgt)[1]
    asset_ret <- cut_time(res$asset_ret, date_start)
  }
  miss <- sum(is.na(asset_ret))
  if (miss > 0) {
    warning(paste0(miss, ' NAs found in asset_ret. Replacing with 0s'))
  }
  asset_ret[is.na(asset_ret)] <- 0
  p <- Portfolio$new(
    name = nm,
    asset_ret = asset_ret,
    reb_wgt = wgt
  )
  p$lazy_reb_wgt(reb_freq = reb_freq)
  p$rebal()
  return(p)
}


#' @export
df_to_wgt <- function(df) {
  is_miss <- is.na(df$ReturnCol)
  if (any(is_miss)) {
    if (all(is_miss)) {
      stop('no ReturnCol data found')
    }
    warning('NAs found in ReturnCol, removing before pivot_wider')
    df <- df[!is_miss, ]
  }
  wgt <- tidyr::pivot_wider(df, id_cols = returnInfo, names_from = ReturnCol,
                            values_from = pctVal)
  mat_to_xts(wgt)
} 


#' @param ret list of returns, ret$d = daily, ret$m = monthly
#' @export
match_ret_df <- function(mdf, ret) {
  # TO-DO: handle mix of monthly and daily returns
  freq <- unique(mdf$match$ReturnLib)
  ret_col <- unique(mdf$match$ReturnCol)
  if ('monthly' %in% freq) {
    month_bool <- TRUE
  } else {
    month_bool <- FALSE
    ix <- match(ret_col, colnames(ret$d), incomparables = NA)
    if (any(is.na(ix))) {
      miss_col <- ret_col[is.na(ix)] %in% mdf$match$ReturnCol
      mdf$miss <- rob_rbind(mdf$miss, mdf$match[miss_col, ])
      mdf$match <- mdf$match[-miss_col, ]
      if (all(is.na(ix))) {
        stop('no returns found')
      }
      ix <- na.omit(ix)
    }
  }
  if (month_bool) {
    ix <- match(ret_col, colnames(ret$m), incomparables = NA)
    if (any(is.na(ix))) {
      miss_col <- ret_col[is.na(ix)] %in% mdf$match$ReturnCol
      mdf$miss <- rob_rbind(mdf$miss, mdf$match[miss_col, ])
      mdf$match <- mdf$match[-miss_col, ]
    }
    if (all(is.na(ix))) {
      stop('no returns found')
    }
    ix <- na.omit(ix)
    asset_ret <- ret$m[, ix]
  } else {
    asset_ret <- ret$d[, ix]
  }
  res <- list()
  res$mdf <- mdf
  res$asset_ret <- asset_ret
  return(res)
}


#' @title Read Piper Sandler Macro Select Workbook
#' @param wb workbook full file name including path
#' @param idx string representing which index to use, e.g., "Russell 3000"
#' @param fact_nm string vector representing names of the current macro factors
#' @export
read_macro_wb <- function(wb, idx, fact_nm) {
  menu <- readxl::read_excel(wb, 'menu')
  menu <- as.data.frame(menu)
  col_off <- menu[menu[, 2] == idx, 3]
  col_off <- na.omit(col_off)
  dat <- readxl::read_excel(wb, 'data', skip = 4)
  model <- dat[, c(1:7, (col_off-1):(col_off+3))]
  model <- as.data.frame(model)
  colnames(model)[8:12] <- fact_nm
  return(model)
}


#' @title Match securities in a portfolio with the Master Security List
#' @param df data.frame of securities, see details
#' @param msl Master Security List
#' @details
#' The match will attempt to match the following `df` columns
#'   ticker, cusip, name, isin, lei, and identifier
#' @return numeric vector with row numbers of the match in the `msl`
#' @export
msl_match <- function(df, msl) {
  incomps <- c(NA, "000000000", "N/A", "0")
  ix_ticker <- match(df$ticker, msl$Ticker, incomparables = incomps)
  ix_cusip <- match(df$cusip, msl$CUSIP, incomparables = incomps)
  ix_name <- match(toupper(df$name), toupper(msl$DTCName), incomparables = incomps)
  ix_isin <- match(df$isin, msl$ISIN, incomparables = incomps)
  ix_lei <- match(df$lei, msl$LEI, incomparables = incomps)
  ix_idnt <- match(df$identifier, msl$Identifier, incomparables = incomps)
  ix_idnt2 <- match(df$identifier, msl$SEDOL, incomparables = incomps)
  ix_idnt3 <- match(df$identifier, msl$LEI, incomparables = incomps)
  ix_idnt4 <- match(df$identifier, msl$CUSIP, incomparables = incomps)
  ix_asset_id <- match(df$assetId, msl$BDAssetID, incomparables = incomps)
  ix <- rep(NA, nrow(df))
  ix <- .fill_ix(ix, ix_ticker)
  ix <- .fill_ix(ix, ix_cusip)
  ix <- .fill_ix(ix, ix_name)
  ix <- .fill_ix(ix, ix_isin)
  ix <- .fill_ix(ix, ix_lei)
  ix <- .fill_ix(ix, ix_idnt)
  ix <- .fill_ix(ix, ix_idnt2)
  ix <- .fill_ix(ix, ix_idnt3)
  ix <- .fill_ix(ix, ix_idnt4)
  ix <- .fill_ix(ix, ix_asset_id)
  return(ix)
}


#' @export
.fill_ix <- function(a, b) {
  if (length(a) == length(b)) {
    a[is.na(a)] <- b[is.na(a)]
    return(a)
  } else {
    return(a)
  }
}



#' @export
merge_msl <- function(df, msl) {
  ix <- msl_match(df, msl)
  union_df <- cbind(df, msl[ix, ])
  inter_df <- union_df[!is.na(ix), ]
  miss_df <- union_df[is.na(ix), ]
  res <- list(
    all = union_df,
    match = inter_df,
    miss = miss_df
  )
  return(res)
}


#' @export
drill_down <- function(mdf, msl, bucket, latest = TRUE) {
  check_mdf(mdf)
  df <- mdf$match
  res <- list(match = data.frame(), miss = data.frame())
  for (i in 1:nrow(df)) {
    if (df$Layer[i] > 1) {
      df_i <- read_holdings_file(bucket, df$DTCName[i], latest)
      df_i$ParrentAsset <- df$DTCName[i]
      mdf_i <- merge_msl(df_i, msl)
      mdf_i$match$pctVal <- mdf_i$match$pctVal * df$pctVal[i]
      mdf_i$miss$pctVal <- mdf_i$miss$pctVal * df$pctVal[i]
      res$match <- rob_rbind(res$match, mdf_i$match)
      res$miss <- rob_rbind(res$miss, mdf_i$miss)
    } else {
      res$match <- rob_rbind(res$match, df[i, ])
    }
  }
  return(res)
}


#' @export
expand_all <- function(mdf, msl, bucket, latest = TRUE) {
  max_layer <- 4
  res <- drill_down(mdf, msl, bucket)
  for (i_layer in 1:max_layer) {
    if (max(res$match$Layer, na.rm = TRUE) == 1) {
      break
    }
    ix <- res$match$Layer > 1
    ix[is.na(ix)] <- FALSE
    expand_df <- res$match[ix, ]
    res$match <- res$match[!ix, ]
    for (i_row in 1:nrow(expand_df)) {
      df_i <- read_holdings_file(bucket, expand_df$DTCName[i_row], latest)
      mdf_i <- merge_msl(df_i, msl)
      mdf_i$match$pctVal <- expand_df$pctVal[i_row] * mdf_i$match$pctVal
      mdf_i$miss$pctVal <- expand_df$pctVal[i_row] * mdf_i$miss$pctVal
      res$match <- rob_rbind(res$match, mdf_i$match)
      res$miss <- rob_rbind(res$miss, mdf_i$miss)
    }
  }
  return(res)
}


#' @export
read_holdings_file <- function(bucket, dtc_name, latest = FALSE) {
  s3_path <- paste0("holdings/", dtc_name, ".parquet")
  s3_exists <- s3_path %in% bucket$ls("holdings/")
  if (s3_exists) {
    df <- read_parquet(bucket$path(s3_path))
    if (latest) {
      df <- latest_holdings(df)
    }
    return(df)
  } else {
    warning(paste0(s3_path), " not found.")
    return()
  }
}


#' @export
rob_rbind <- function(df1, df2) {
  if (nrow(df1) == 0) {
    return(df2)
  }
  if (nrow(df2) == 0) {
    return(df1)
  }
  nm_union <- unique(c(colnames(df1), colnames(df2)))
  df1_miss <- !nm_union %in% colnames(df1)
  df2_miss <- !nm_union %in% colnames(df2)
  df1[, nm_union[df1_miss]] <- NA
  df2[, nm_union[df2_miss]] <- NA
  df2 <- df2[, colnames(df1)]
  rbind(df1, df2)
}


#' @export
check_mdf <- function(mdf) {
  if (!is.list(mdf)) {
    stop('mdf is not a list')
  }
  if (!all(c("all", "match", "miss") %in% names(mdf))) {
    stop("mdf slots not properly set up")
  }
  return("mdf pass")
}


#' @export
latest_holdings <- function(df) {
  if (any(is.na(df$returnInfo))) {
    warning('some returnInfo missing')
  }
  if ('ParrentAsset' %in% colnames(df)) {
    pa <- na.omit(unique(df$ParrentAsset))
    for (i in 1:length(pa)) {
      is_pa <- df$ParrentAsset == pa[i]
      is_pa[is.na(is_pa)] <- FALSE
      df_pa <- df[is_pa, ]
      is_old <- df_pa[, 'returnInfo'] < max(df_pa[, 'returnInfo'], na.rm = TRUE)
      df_pa <- df_pa[!is_old, ]
      if (i == 1) {
        res <- df_pa
      } else {
        res <- rbind(res, df_pa)
      }
    }
  } else {
    ix <- df$returnInfo == max(df$returnInfo, na.rm = TRUE)
    if (any(is.na(ix))) {
      warning('some returnInfo missing')
      ix[is.na(ix)] <- FALSE
    }
    res <- df[ix, ]
  }
  return(res)
}


#' @export
port_expand_all <- function(dtc_name, bucket, msl, latest = TRUE) {
  df <- read_holdings_file(bucket, dtc_name, latest)
  mdf <- merge_msl(df, msl)
  exp_df <- expand_all(mdf, msl, bucket, latest)
  if (latest) {
    exp_df$match <- latest_holdings(exp_df$match)
    exp_df$miss <- latest_holdings(exp_df$miss)
  }
  return(exp_df)
}


