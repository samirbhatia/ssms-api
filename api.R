library(plumber)
library(DBI)
library(duckdb)
library(jsonlite)
library(digest)
library(httr)

# =========================================================
# Environment helpers
# =========================================================

FM_HOST <- Sys.getenv("FM_HOST")
FM_FILE <- Sys.getenv("FM_FILE")
FM_USER <- Sys.getenv("FM_USER")
FM_PASSWORD <- Sys.getenv("FM_PASSWORD")

if (FM_HOST == "" || FM_FILE == "" || FM_USER == "" || FM_PASSWORD == "") {
  stop("❌ FileMaker environment variables not fully set")
}

# =========================================================
# Razorpay signature verification
# =========================================================

verify_razorpay_signature <- function(raw_body, received_sig) {
  secret <- Sys.getenv("RAZORPAY_WEBHOOK_SECRET")
  if (secret == "") stop("RAZORPAY_WEBHOOK_SECRET not set")
  
  expected_sig <- digest::hmac(
    key = secret,
    object = raw_body,
    algo = "sha256",
    serialize = FALSE
  )
  
  identical(received_sig, expected_sig)
}

# =========================================================
# FileMaker helpers
# =========================================================

fm_login <- function() {
  res <- httr::POST(
    paste0(
      FM_HOST,
      "/fmi/data/vLatest/databases/",
      FM_FILE,
      "/sessions"
    ),
    httr::authenticate(FM_USER, FM_PASSWORD),
    httr::add_headers("Content-Type" = "application/json"),
    body = "{}",
    encode = "raw",
    httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  httr::stop_for_status(res)
  httr::content(res)$response$token
}

# ---- idempotency check (correct FileMaker _find) ----
fm_payment_exists <- function(token, payment_id) {
  
  url <- paste0(
    FM_HOST,
    "/fmi/data/vLatest/databases/",
    FM_FILE,
    "/layouts/razor/_find"
  )
  
  res <- httr::POST(
    url,
    httr::add_headers(
      Authorization = paste("Bearer", token),
      "Content-Type" = "application/json"
    ),
    body = list(
      query = list(list(payment_id = payment_id)),
      limit = 1
    ),
    encode = "json",
    httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  status <- httr::status_code(res)
  
  if (status == 401) return(FALSE)   # no match
  if (status == 200) return(TRUE)
  
  stop("❌ FileMaker find failed: ", httr::content(res, as = "text"))
}

# ---- safe insert with logging ----
fm_insert_razor <- function(token, record) {
  
  res <- httr::POST(
    paste0(
      FM_HOST,
      "/fmi/data/vLatest/databases/",
      FM_FILE,
      "/layouts/razor/records"
    ),
    httr::add_headers(
      Authorization = paste("Bearer", token),
      "Content-Type" = "application/json"
    ),
    body = list(fieldData = record),
    encode = "json",
    httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  if (httr::status_code(res) != 200) {
    message("❌ FileMaker insert failed")
    print(httr::content(res, as = "parsed"))
    stop("FileMaker insert rejected")
  }
  
  invisible(TRUE)
}

# =========================================================
# Load MotherDuck data at startup
# =========================================================

DATA <- NULL

load_data <- function() {
  tryCatch({
    message("➡️ Starting load_data()")
    
    con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    dbExecute(con, "INSTALL motherduck;")
    dbExecute(con, "LOAD motherduck;")
    dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
    
    df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
    dbDisconnect(con, shutdown = TRUE)
    
    message("🎉 Loaded ", nrow(df), " rows from MotherDuck")
    as.data.frame(df)
  }, error = function(e) {
    message("❌ load_data failed: ", e$message)
    NULL
  })
}

DATA <- load_data()

# =========================================================
# CORS
# =========================================================

#* @filter cors
function(req, res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, X-Razorpay-Signature")
  
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  }
  plumber::forward()
}

# =========================================================
# Health
# =========================================================

#* @get /health
function() {
  list(
    status = "ok",
    rows = if (is.data.frame(DATA)) nrow(DATA) else NA
  )
}

# =========================================================
# Search API
# =========================================================

#* @get /search
#* @param name
#* @param admission
#* @param school
function(name = "", admission = "", school = "Janakpuri", res) {
  
  if (!is.data.frame(DATA)) {
    res$status <- 500
    return(list(error = "DATA not loaded"))
  }
  
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Enter at least 3 characters"))
  }
  
  nm  <- tolower(trimws(name))
  adm <- tolower(trimws(admission))
  sch <- tolower(trimws(school))
  
  df <- DATA[
    grepl(nm, tolower(DATA$student_name), fixed = TRUE) &
      grepl(adm, tolower(DATA$admission_number), fixed = TRUE) &
      tolower(trimws(DATA$school_full)) == sch,
  ]
  
  head(df, 50)
}

# =========================================================
# Razorpay Webhook (PRODUCTION SAFE)
# =========================================================

#* @post /razorpay/webhook
#* @serializer json
function(req, res) {
  
  message("🔥 Razorpay webhook hit")
  
  sig <- req$HTTP_X_RAZORPAY_SIGNATURE
  if (is.null(sig)) {
    res$status <- 400
    return(list(error = "Missing Razorpay signature"))
  }
  
  raw_body <- req$postBody
  if (is.null(raw_body) || nchar(raw_body) == 0) {
    res$status <- 400
    return(list(error = "Empty body"))
  }
  
  if (!verify_razorpay_signature(raw_body, sig)) {
    res$status <- 401
    return(list(error = "Invalid Razorpay signature"))
  }
  
  payload <- jsonlite::fromJSON(raw_body, simplifyVector = FALSE)
  
  if (payload$event != "payment.captured") {
    return(list(status = "ignored"))
  }
  
  payment <- payload$payload$payment$entity
  payment_id <- payment$id
  
  token <- fm_login()
  
  if (fm_payment_exists(token, payment_id)) {
    message("⚠️ Duplicate ignored: ", payment_id)
    return(list(status = "duplicate"))
  }
  
  # ---- ONLY validation-safe fields ----
  record <- list(
    payment_id       = payment$id,
    order_id         = payment$order_id,
    `payment status` = payment$status,
    admission_number = payment$notes$admission_number,
    student_name     = payment$notes$student_name,
    branch           = payment$notes$branch,
    email            = payment$email,
    phone            = payment$contact
  )
  
  fm_insert_razor(token, record)
  
  message("✅ Payment inserted: ", payment_id)
  list(status = "ok")
}