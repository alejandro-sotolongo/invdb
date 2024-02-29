# library(DBI)
# library(arrow)
# library(nanoarrow)
# library(fst)
#
# load('test.RData')
#
# con <- dbConnect(
#   odbc::odbc(),
#   driver = 'SQL Server',
#   server = 'DTC-2K22-INVEST',
#   UID = 'DTC-NET\asotolongo',
#   PWD = pwd,
#   database = 'test_db',
#   Trusted_Connection = 'yes'
# )
#
# us_stocks <- fst::read_fst('C:/db/Returns/us_stocks.fst')
# us_stocks <- as.data.frame(us_stocks)
#
# us_stocks <- us_stocks[us_stocks$Index > as.Date('2022-01-01/'), ]
# us_stocks_tidy <- tidyr::pivot_longer(us_stocks, cols = -1)
#
# ptm <- proc.time()
# x <- seq(1, 1400000, 100000)
# for (i in 1:(length(x)-1)) {
#   dbWriteTable(con, 'us_stocks', us_stocks_tidy[x[i]:x[i+1], ], append = TRUE)
#   print(i)
# }
# y <- proc.time() - ptm
#
# ptm <- proc.time()
# dat <- dbReadTable(con, 'us_stocks')
# sql_read <- proc.time() - ptm
#
# write_feather(as_arrow_table(us_stocks_tidy), 'N:/Investment Team/test.feather')
#
# ptm <- proc.time()
# dat2 <- read_feather('N:/Investment Team/test.feather')
# fr <- proc.time() - ptm
#
# ptm <- proc.time()
