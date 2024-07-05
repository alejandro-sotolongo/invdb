
#' @title Download CTF Daily estimates from FactSet
#' @export
download_ctf_daily <- function() {
  factset <- subset_df(db$msl, 'ReturnSource', 'ctf_d')
  res <- list()
  for (i in 1:nrow(factset)) {
    id <- paste0("CLIENT:/PA_SOURCED_RETURNS/", factset$ISIN[i])
    x <- try(download_fs_ctf_ret(id, db$api_keys, t_minus = '-5'))
    if ('try-error' %in% class(x)) {
      next
    }
    res[[i]] <- x
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
  hist_ret <- read_parquet(db$bucket$path("returns/daily/ctf_d.parquet"))
  hist_ret <- xts(hist_ret[, -1], hist_ret[[1]])
  combo <- xts_rbind(hist_ret, new_ret)
  combo <- xts_to_dataframe(combo)
  write_parquet(combo, db$bucket$path("returns/daily/ctf_d.parquet"))
}