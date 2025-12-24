library(plumber)
library(DBI)
library(duckdb)


#* @get /
#* @serializer html
function() {
  paste(readLines("index.html"), collapse = "\n")
}
# ---- CORS filter ----
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

# ---- DB connection ----
get_con <- function() {
  token <- Sys.getenv("MOTHERDUCK_TOKEN")
  if (token == "") stop("MOTHERDUCK_TOKEN not set in environment")
  
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  DBI::dbExecute(con, "INSTALL motherduck;")
  DBI::dbExecute(con, "LOAD motherduck;")
  
  DBI::dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
  
  con
}

# ---- Health check ----
#* @get /health
function() {
  list(status = "ok")
}

# ---- Search API ----
#* @get /search
#* @param name Student name (min 3 chars)
#* @param admission Admission number (min 3 chars)
#* @param school School/branch name
#* #* @serializer json
function(name = "", admission = "", school = "Janakpuri") {
  
  name <- trimws(name)
  admission <- trimws(admission)
  school <- trimws(school)
  
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Enter at least 3 characters for both Name and Admission Number"))
  }
  
  con <- get_con()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  
  sql <- "
    SELECT student_name, student_class, student_section,
           admission_number, balance, payment_link
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY admission_number
               ORDER BY balance DESC
             ) AS rn
      FROM ssms.vw_balances
      WHERE school_full = ?
        AND student_name ILIKE ?
        AND admission_number ILIKE ?
    )
    WHERE rn = 1
    LIMIT 50
  "
  
  dbGetQuery(
    con, sql,
    params = list(
      school,
      paste0('%', name, '%'),
      paste0('%', admission, '%')
    )
  )
}

