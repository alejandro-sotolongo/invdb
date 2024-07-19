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
