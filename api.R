library(plumber)
library(DBI)
library(duckdb)

DATA <- NULL

# ---- Load data ONCE at startup ----
load_data <- function() {
  message("Loading data from MotherDuck...")
  
  token <- Sys.getenv("MOTHERDUCK_TOKEN")
  if (token == "") stop("MOTHERDUCK_TOKEN not set")
  
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  dbExecute(con, "INSTALL motherduck;")
  dbExecute(con, "LOAD motherduck;")
  dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
  
  df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
  dbDisconnect(con, shutdown = TRUE)
  
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
  list(status = "ok", rows = nrow(DATA))
}

# ---- Search ----
#* @get /search
#* @param name
#* @param admission
#* @param school
function(name = "", admission = "", school = "Janakpuri", res) {
  
  name <- trimws(name)
  admission <- trimws(admission)
  school <- trimws(school)
  
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Enter at least 3 characters for Name and Admission"))
  }
  
  df <- DATA[
    grepl(tolower(name), tolower(DATA$student_name)) &
      grepl(tolower(admission), tolower(DATA$admission_number)) &
      DATA$school_full == school,
  ]
  
  head(df, 50)
}