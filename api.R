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
# SAFE nested accessor (CRITICAL FIX)
# =========================================================

safe_get <- function(x, path, default = "") {
  tryCatch({
    for (p in path) {
      if (is.null(x) || !is.list(x)) return(default)
      x <- x[[p]]
    }
    if (is.null(x) || length(x) == 0) default else x
  }, error = function(e) default)
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

# ---- Idempotency check (FileMaker _find) ----
fm_payment_exists <- function(token, payment_id) {
  
  url <- paste0(
    FM_HOST,
    "/fmi/data/vLatest/databases/",
    FM_FILE,
    "/layouts/razor/_find"
  )
  
  res <- httr::POST(
    url,
    add_headers(
      Authorization = paste("Bearer", token),
      "Content-Type" = "application/json"
    ),
    body = list(
      query = list(list(payment_id = payment_id)),
      limit = 1
    ),
    encode = "json",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  status <- status_code(res)
  
  # Found → duplicate
  if (status == 200) return(TRUE)
  
  # Not found → normal
  if (status %in% c(401, 404, 500)) return(FALSE)
  
  # Unexpected → log only
  message("❌ FileMaker _find unexpected status: ", status)
  message(content(res, as = "text"))
  
  FALSE
}

# ---- Insert with token refresh ----
fm_insert_razor <- function(token, record) {
  
  do_insert <- function(token) {
    POST(
      paste0(
        FM_HOST,
        "/fmi/data/vLatest/databases/",
        FM_FILE,
        "/layouts/razor/records"
      ),
      add_headers(
        Authorization = paste("Bearer", token),
        "Content-Type" = "application/json"
      ),
      body = list(fieldData = record),
      encode = "json",
      config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
    )
  }
  
  res <- do_insert(token)
  if (status_code(res) == 200) return(TRUE)
  
  body <- content(res, as = "parsed", simplifyVector = TRUE)
  
  # Token expired → relogin once
  if (!is.null(body$messages[[1]]$code) && body$messages[[1]]$code == "952") {
    message("🔁 FileMaker token expired — re-authenticating")
    .fm_token <<- NULL
    token <- fm_login()
    res <- do_insert(token)
    if (status_code(res) == 200) return(TRUE)
  }
  
  stop("FileMaker insert failed: ", content(res, as = "text"))
}

# =========================================================
# Load MotherDuck data (search only)
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
# Search API (website)
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
# Razorpay Webhook (FAIL-SAFE & HARDENED)
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
  
  if (safe_get(payload, c("event")) != "payment.captured") {
    return(list(status = "ignored"))
  }
  
  tryCatch({
    
    check_fm_env()
    token <- fm_login()
    
    payment <- safe_get(payload, c("payload", "payment", "entity"), NULL)
    
    # 🚨 HARD GUARD — fixes atomic vector crash forever
    if (is.null(payment) || !is.list(payment)) {
      message("⚠️ Invalid payment structure, skipping webhook")
      res$status <- 200
      return(list(status = "ignored"))
    }
    
    payment_id <- safe_get(payment, c("id"))
    
    if (!fm_payment_exists(token, payment_id)) {
      
      record <- list(
        payment_id = payment_id,
        order_id   = safe_get(payment, c("order_id")),
        `total payment amount` = as.numeric(safe_get(payment, c("amount"), 0)) / 100,
        currency   = safe_get(payment, c("currency")),
        `payment status` = safe_get(payment, c("status")),
        student_name     = safe_get(payment, c("notes", "student_name")),
        admission_number = safe_get(payment, c("notes", "admission_number")),
        branch           = safe_get(payment, c("notes", "branch")),
        email            = safe_get(payment, c("email")),
        phone            = as.character(safe_get(payment, c("contact")))
      )
      
      fm_insert_razor(token, record)
      message("✅ Payment inserted: ", payment_id)
      
    } else {
      message("⚠️ Duplicate ignored: ", payment_id)
    }
    
  }, error = function(e) {
    message("❌ Webhook processing error: ", e$message)
  })
  
  res$status <- 200
  list(status = "ok")
}