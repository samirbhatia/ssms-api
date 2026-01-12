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
# SAFE nested accessor (NO $ EVER)
# =========================================================

safe_get <- function(x, path, default = NULL) {
  for (p in path) {
    if (!is.list(x)) return(default)
    if (!p %in% names(x)) return(default)
    x <- x[[p]]
  }
  x
}

# =========================================================
# FileMaker helpers
# =========================================================

.fm_token <- NULL

fm_login <- function() {
  if (!is.null(.fm_token)) return(.fm_token)
  
  res <- POST(
    paste0(FM_HOST, "/fmi/data/vLatest/databases/", FM_FILE, "/sessions"),
    authenticate(FM_USER, FM_PASSWORD),
    add_headers("Content-Type" = "application/json"),
    body = "{}",
    encode = "raw",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  stop_for_status(res)
  
  parsed <- content(res, as = "parsed", simplifyVector = FALSE)
  token  <- safe_get(parsed, c("response", "token"))
  
  if (!is.character(token)) stop("Failed to obtain FileMaker token")
  
  .fm_token <<- token
  token
}

# ---- Idempotency check ----
fm_payment_exists <- function(token, payment_id) {
  
  res <- POST(
    paste0(FM_HOST, "/fmi/data/vLatest/databases/", FM_FILE, "/layouts/razor/_find"),
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
  
  if (status == 200) return(TRUE)
  if (status %in% c(401, 404, 500)) return(FALSE)
  
  message("❌ FileMaker _find unexpected status: ", status)
  FALSE
}

# ---- Insert with token refresh ----
fm_insert_razor <- function(token, record) {
  
  do_insert <- function(tok) {
    POST(
      paste0(FM_HOST, "/fmi/data/vLatest/databases/", FM_FILE, "/layouts/razor/records"),
      add_headers(
        Authorization = paste("Bearer", tok),
        "Content-Type" = "application/json"
      ),
      body = list(fieldData = record),
      encode = "json",
      config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
    )
  }
  
  res <- do_insert(token)
  if (status_code(res) == 200) return(TRUE)
  
  body_raw <- content(res, as = "text", encoding = "UTF-8")
  body     <- tryCatch(fromJSON(body_raw, simplifyVector = FALSE), error = function(e) NULL)
  
  code <- safe_get(body, c("messages", "1", "code"))
  
  if (identical(code, "952")) {
    message("🔁 FileMaker token expired — re-authenticating")
    .fm_token <<- NULL
    token <- fm_login()
    res <- do_insert(token)
    if (status_code(res) == 200) return(TRUE)
  }
  
  stop("FileMaker insert failed: ", body_raw)
}

# =========================================================
# Load MotherDuck data
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
  list(status = "ok", rows = if (is.data.frame(DATA)) nrow(DATA) else NA)
}

# =========================================================
# Search
# =========================================================

#* @get /search
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
  
  head(DATA[
    grepl(nm, tolower(DATA$student_name), fixed = TRUE) &
      grepl(adm, tolower(DATA$admission_number), fixed = TRUE) &
      tolower(trimws(DATA$school_full)) == sch,
  ], 50)
}

# =========================================================
# Razorpay Webhook (ABSOLUTELY SAFE)
# =========================================================

#* @post /razorpay/webhook
#* @serializer json
function(req, res) {
  
  message("🔥 Razorpay webhook hit")
  
  if (!is.character(req$postBody)) {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  sig <- req$HTTP_X_RAZORPAY_SIGNATURE
  raw <- req$postBody
  
  if (!is.character(sig) || raw == "") {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  if (!verify_razorpay_signature(raw, sig)) {
    res$status <- 200
    return(list(status = "invalid-signature"))
  }
  
  payload <- tryCatch(fromJSON(raw, simplifyVector = FALSE), error = function(e) NULL)
  
  if (!is.list(payload)) {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  event <- safe_get(payload, c("event"))
  if (!identical(event, "payment.captured")) {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  tryCatch({
    
    check_fm_env()
    token <- fm_login()
    
    payment <- safe_get(payload, c("payload", "payment", "entity"))
    if (!is.list(payment)) return()
    
    payment_id <- safe_get(payment, c("id"))
    if (!is.character(payment_id)) return()
    
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