% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wrangle_holdings.R
\name{drill_down}
\alias{drill_down}
\title{Drilldown one layer}
\usage{
drill_down(mdf, msl, bucket, latest = TRUE, lay = 2, recons_wgt = TRUE)
}
\arguments{
\item{mdf}{list from `merge_msl`}

\item{bucket}{s3 bucket from Database object}

\item{latest}{boolean for use latest assets only}

\item{lay}{which layer to drill down}

\item{recons_wgt}{boolean to force weights to sum to 1}
}
\description{
Drilldown one layer
}
