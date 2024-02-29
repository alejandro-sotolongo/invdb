port_from_df <- function(df, nm = 'Port') {

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
