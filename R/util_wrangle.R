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
