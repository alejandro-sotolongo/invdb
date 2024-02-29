#' @title Read master security excel file and save fst file
#' @export
update_msl <- function() {
  msl <- readxl::read_excel('N:/Investment Team/DATABASES/MDB/Tables/msl.xlsx', 'msl')
  fst::write_fst(msl, 'N:/Investment Team/DATABASES/MDB/Tables/msl.fst')
}

#' @title Read in MSL table
#' @export
read_msl <- function() {
  fst::read_fst('N:/Investment Team/DATABASES/MDB/Tables/msl.fst')
}

#' @title Update geographies excel file and save fst file
#' @export
update_geo <- function() {
  geo <- readxl::read_excel('N:/Investment Team/DATABASES/MDB/Tables/geo.xlsx', 'geo')
  fst::write_fst(geo, 'N:/Investment Team/DATABASES/MDB/Tables/geo.fst')
}

#' @title Read in geograpies table
#' @export
read_geo <- function() {
  fst::read_fst('N:/Investment Team/DATABASES/MDB/Tables/geo.fst')
}

