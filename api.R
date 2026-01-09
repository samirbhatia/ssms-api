library(plumber)
library(DBI)
library(duckdb)
library(jsonlite)
library(digest)
library(httr)

# =========================================================
# Environment (DO NOT STOP AT LOAD TIME)
# =========================================================

FM_HOST     <- Sys.getenv("FM_HOST")
FM_FILE     <- Sys.getenv("FM_FILE")
FM_USER     <- Sys.getenv("FM_USER")
FM_PASSWORD <- Sys.getenv("FM_PASSWORD")

check_fm_env <- function() {
  if (FM_HOST == "" || FM_FILE == "" || FM_USER == "" || FM_PASSWORD == "") {
    stop("FileMaker environment variables not set")
  }
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

.fm_token <- NULL

fm_login <- function() {
  if (!is.null(.fm_token)) return(.fm_token)
  
  res <- httr::POST(
    paste0(
      FM_HOST,
      "/fmi/data/vLatest/databases/",
      FM_FILE,
      "/sessions"
    ),
    authenticate(FM_USER, FM_PASSWORD),
    add_headers("Content-Type" = "application/json"),
    body = "{}",
    encode = "raw",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  httr::stop_for_status(res)
  .fm_token <<- content(res)$response$token
  .fm_token
}

# ---- idempotency check (correct FileMaker find) ----
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
    httr::config(
      ssl_verifypeer = FALSE,
      ssl_verifyhost = FALSE
    )
  )
  
  status <- httr::status_code(res)
  
  if (status == 200) {
    return(TRUE)      # duplicate payment
  }
  
  if (status == 401) {
    return(FALSE)     # NEW payment — THIS IS NORMAL
  }
  
  # only real errors reach here
  stop("FileMaker _find failed: ", httr::content(res, as = "text"))
}

fm_insert_razor <- function(token, record) {
  
  do_insert <- function(token) {
    httr::POST(
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
  }
  
  res <- do_insert(token)
  
  if (httr::status_code(res) == 200) return(TRUE)
  
  body <- httr::content(res, as = "parsed", simplifyVector = TRUE)
  
  # 🔁 Token expired → relogin once
  if (!is.null(body$messages[[1]]$code) && body$messages[[1]]$code == "952") {
    message("🔁 FileMaker token expired — re-authenticating")
    .fm_token <<- NULL
    token <- fm_login()
    res <- do_insert(token)
    
    if (httr::status_code(res) == 200) return(TRUE)
  }
  
  stop("FileMaker insert failed: ", httr::content(res, as = "text"))
}

# =========================================================
# Load MotherDuck data at startup (search only)
# =========================================================

DATA <- NULL

load_data <- function() {
  tryCatch({
    message("➡️ Loading MotherDuck data")
    
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    dbExecute(con, "INSTALL motherduck;")
    dbExecute(con, "LOAD motherduck;")
    dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
    
    df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
    dbDisconnect(con, shutdown = TRUE)
    
    message("🎉 Loaded ", nrow(df), " rows")
    df
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
# Search API (WEBSITE)
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
# Razorpay Webhook (FAIL-SAFE)
# =========================================================

#* @post /razorpay/webhook
#* @serializer json
function(req, res) {
  
  message("🔥 Razorpay webhook hit")
  
  sig <- req$HTTP_X_RAZORPAY_SIGNATURE
  raw_body <- req$postBody
  
  if (is.null(sig) || is.null(raw_body) || raw_body == "") {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  if (!verify_razorpay_signature(raw_body, sig)) {
    res$status <- 200
    return(list(status = "invalid-signature"))
  }
  
  payload <- fromJSON(raw_body, simplifyVector = FALSE)
  
  if (payload$event != "payment.captured") {
    return(list(status = "ignored"))
  }
  
  tryCatch({
    
    check_fm_env()
    token <- fm_login()
    
    payment <- payload$payload$payment$entity
    
    if (!fm_payment_exists(token, payment$id)) {
      
      safe <- function(x) if (is.null(x) || length(x) == 0) "" else x
      
      record <- list(
        payment_id = safe(payment$id),
        order_id   = safe(payment$order_id),
        `total payment amount` = as.numeric(payment$amount) / 100,
        currency   = safe(payment$currency),
        `payment status` = safe(payment$status),
        student_name = safe(payment$notes$student_name),
        admission_number = safe(payment$notes$admission_number),
        branch = safe(payment$notes$branch),
        email  = safe(payment$email),
        phone  = as.character(safe(payment$contact))
      )
      
      fm_insert_razor(token, record)
      message("✅ Payment inserted: ", payment$id)
    } else {
      message("⚠️ Duplicate ignored: ", payment$id)
    }
    
  }, error = function(e) {
    message("❌ Webhook processing error: ", e$message)
  })
  
  res$status <- 200
  list(status = "ok")
}