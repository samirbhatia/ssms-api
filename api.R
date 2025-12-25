.cache <- new.env(parent = emptyenv())
CACHE_TTL <- 300  # 5 minutes

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
  
  # install/load only once per session
  try(DBI::dbExecute(con, "INSTALL motherduck;"), silent = TRUE)
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
function(name = "", admission = "", school = "Janakpuri", res) {
  
  name <- trimws(name)
  admission <- trimws(admission)
  school <- trimws(school)
  
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Enter at least 3 characters for both Name and Admission Number"))
  }
  
  ## ✅ define key first
  key <- paste(tolower(school), tolower(name), tolower(admission), sep = "|")
  now <- Sys.time()
  
  ## ✅ cache hit
  if (exists(key, envir = .cache)) {
    entry <- get(key, envir = .cache)
    if (difftime(now, entry$time, units = "secs") < CACHE_TTL) {
      message("Cache hit: ", key)
      return(entry$data)
    } else {
      rm(list = key, envir = .cache)
    }
  }
  
  message("Cache miss: ", key)
  
  ## DB query
  con <- get_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  sql <- "
    SELECT *
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY admission_number
               ORDER BY admission_number
             ) AS rn
      FROM students
      WHERE school_full = ?
        AND student_name ILIKE ?
        AND admission_number ILIKE ?
    )
    WHERE rn = 1
    LIMIT 50
  "
  
  df <- DBI::dbGetQuery(
    con, sql,
    params = list(
      school,
      paste0('%', name, '%'),
      paste0('%', admission, '%')
    )
  )
  
  ## ✅ store in cache
  assign(key, list(time = now, data = df), envir = .cache)
  
  df
}