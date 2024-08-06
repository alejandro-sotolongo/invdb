try_merge <- function(x, y, by, all = TRUE) {
  combo <- try(merge(x, y, by, all), silent = 'TRUE')
  return(combo)
}

#' @export
subset_df <- function(df, col, target) {
  ix <- df[, col] == target
  ix[is.na(ix)] <- FALSE
  return(df[ix, ])
}


#' @export
extract_list <- function(x, nm) {
  y <- lapply(x, '[[', nm)
  y[sapply(y, is.null)] <- NA
  unlist(y)
}

#' @export
list_replace_null <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

#' @export
month_end <- function(dt) {
  lubridate::ceiling_date(as.Date(dt), 'months') - 1
}


#' @export
request_id_match_msl <- function(request_id, msl) {
  incomps <- c(NA, "000000000", "N/A", "0")
  ix_isin <- match(request_id, msl$ISIN, incomparables = incomps)
  ix_cusip <- match(request_id, msl$CUSIP, incomparables = incomps)
  ix_sedol <- match(request_id, msl$SEDOL, incomparables = incomps)
  ix_lei <- match(request_id, msl$LEI, incomparables = incomps)
  ix_id <- match(request_id, msl$Identifier, incomparables = incomps)
  ix <- rep(NA, length(request_id))
  ix <- .fill_ix(ix, ix_isin)
  ix <- .fill_ix(ix, ix_cusip)
  ix <- .fill_ix(ix, ix_sedol)
  ix <- .fill_ix(ix, ix_lei)
  ix <- .fill_ix(ix, ix_id)
  return(ix)
}

#' @export
get_iter <- function(ids, max_iter_by = 50) {
  if (length(ids) > max_iter_by) {
    iter <- iter <- seq(1, length(ids), (max_iter_by-1))
    if (iter[length(iter)] < length(ids)) {
      iter <- c(iter, length(ids))
    }
    ret_list <- list()
  } else {
    iter <- c(1, length(ids))
  }
  return(iter)
}
