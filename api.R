library(plumber)
library(DBI)
library(duckdb)

DATA <- NULL

# ---- Load data ONCE at startup ----
load_data <- function() {
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  dbExecute(con, "INSTALL motherduck;")
  dbExecute(con, "LOAD motherduck;")
  dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
  
  df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
  dbDisconnect(con, shutdown = TRUE)
  
  df <- as.data.frame(df)
  
  message("Loaded ", nrow(df), " rows into memory")
  df
}

DATA <<- load_data()

# ---- Serve frontend ----
#* @get /
#* @serializer html
function() {
  paste(readLines("index.html"), collapse = "\n")
}

# ---- Health ----
#* @get /health
function() {
  list(
    status = "ok",
    class = class(DATA),
    rows = if (is.data.frame(DATA)) nrow(DATA) else NA,
    cols = if (is.data.frame(DATA)) ncol(DATA) else NA
  )
}

# ---- Search ----
#* @get /search
#* @param name
#* @param admission
#* @param school
#* @get /search
function(name = "", admission = "", school = "Janakpuri", res) {
  
  if (!is.data.frame(DATA) || nrow(DATA) == 0) {
    res$status <- 500
    return(list(error = "DATA not loaded"))
  }
  
  name <- trimws(name)
  admission <- trimws(admission)
  school <- trimws(school)
  
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Enter at least 3 characters for Name and Admission"))
  }
  
  nm <- tolower(name)
  adm <- tolower(admission)
  sch <- tolower(school)
  
  m1 <- !is.na(DATA$student_name) &
    grepl(nm, tolower(DATA$student_name), fixed = TRUE)
  
  m2 <- !is.na(DATA$admission_number) &
    grepl(adm, tolower(DATA$admission_number), fixed = TRUE)
  
  m3 <- !is.na(DATA$school_full) &
    tolower(trimws(DATA$school_full)) == sch
  
  df <- DATA[m1 & m2 & m3, ]
  
  head(df, 50)
} 