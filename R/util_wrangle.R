try_merge <- function(x, y, by, all = TRUE) {
  combo <- try(merge(x, y, by, all), silent = 'TRUE')
  return(combo)
}
