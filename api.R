library(plumber)
library(DBI)
library(duckdb)

DATA <- NULL

# ---- Load data once at startup ----
load_data <- function() {
  tryCatch({
    message("➡️ Starting load_data()")
    
    token <- Sys.getenv("MOTHERDUCK_TOKEN")
    if (token == "") stop("MOTHERDUCK_TOKEN not set")
    
    message("✅ Token present")
    
    con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    message("✅ DuckDB connected")
    
    dbExecute(con, "INSTALL motherduck;")
    dbExecute(con, "LOAD motherduck;")
    message("✅ MotherDuck extension loaded")
    
    dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
    message("✅ Attached md:ssms_school")
    
    df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
    message("✅ Query done")
    
    dbDisconnect(con, shutdown = TRUE)
    
    df <- as.data.frame(df)
    message("🎉 Loaded ", nrow(df), " rows")
    
    df
  }, error = function(e) {
    message("❌ load_data failed: ", e$message)
    NULL
  })
}

# 👇 force into global env
DATA <- load_data()

# ---- CORS (for Squarespace later) ----
#* @filter cors
function(req, res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type")
  
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  } else {
    plumber::forward()
  }
}

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
    class  = class(DATA),
    rows   = if (is.data.frame(DATA)) nrow(DATA) else NULL,
    cols   = if (is.data.frame(DATA)) ncol(DATA) else NULL
  )
}

# ---- Search ----
#* @get /search
#* @param name
#* @param admission
#* @param school
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
  
  nm  <- tolower(name)
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