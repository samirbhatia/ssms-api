library(plumber)
library(jsonlite)
library(digest)
library(httr)
library(DBI)
library(duckdb)

# =========================================================
# ENV
# =========================================================

FM_HOST     <- Sys.getenv("FM_HOST")
FM_FILE     <- Sys.getenv("FM_FILE")
FM_USER     <- Sys.getenv("FM_USER")
FM_PASSWORD <- Sys.getenv("FM_PASSWORD")

check_fm_env <- function() {
  if (FM_HOST == "" || FM_FILE == "" || FM_USER == "" || FM_PASSWORD == "") {
    stop("FileMaker env vars missing")
  }
}

# =========================================================
# SAFE ACCESS (NO $)
# =========================================================

safe_get <- function(x, path, default = NULL) {
  for (p in path) {
    if (!is.list(x)) return(default)
    if (!p %in% names(x)) return(default)
    x <- x[[p]]
  }
  x
}

num <- function(x) {
  if (is.null(x) || is.na(x)) return(0)
  as.numeric(x)
}

# =========================================================
# RAZORPAY SIGNATURE
# =========================================================

verify_razorpay_signature <- function(raw_body, sig) {
  secret <- Sys.getenv("RAZORPAY_WEBHOOK_SECRET")
  expected <- digest::hmac(
    key = secret,
    object = raw_body,
    algo = "sha256",
    serialize = FALSE
  )
  identical(sig, expected)
}

# =========================================================
# FILEMAKER HELPERS
# =========================================================

fm_login <- function() {
  res <- POST(
    paste0(FM_HOST, "/fmi/data/vLatest/databases/", FM_FILE, "/sessions"),
    authenticate(FM_USER, FM_PASSWORD),
    add_headers("Content-Type" = "application/json"),
    body = "{}",
    encode = "raw",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  stop_for_status(res)
  content(res)$response$token
}

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
  status_code(res) == 200
}

fm_insert <- function(record) {
  token <- fm_login()
  res <- POST(
    paste0(FM_HOST, "/fmi/data/vLatest/databases/", FM_FILE, "/layouts/razor/records"),
    add_headers(
      Authorization = paste("Bearer", token),
      "Content-Type" = "application/json"
    ),
    body = list(fieldData = record),
    encode = "json",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  if (status_code(res) != 200) {
    stop("FileMaker insert failed: ", content(res, as = "text"))
  }
}

# =========================================================
# SEARCH DATA (MotherDuck)
# =========================================================

DATA <- NULL
DATA <- tryCatch({
  con <- dbConnect(duckdb(), dbdir = ":memory:")
  dbExecute(con, "INSTALL motherduck;")
  dbExecute(con, "LOAD motherduck;")
  dbExecute(con, "ATTACH 'md:ssms_school' AS ssms")
  df <- dbGetQuery(con, "SELECT * FROM ssms.vw_balances")
  dbDisconnect(con, shutdown = TRUE)
  df
}, error = function(e) NULL)

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
# HEALTH
# =========================================================

#* @get /health
function() {
  list(status = "ok", rows = if (is.data.frame(DATA)) nrow(DATA) else NA)
}

# =========================================================
# SEARCH
# =========================================================

#* @get /search
function(name = "", admission = "", school = "Janakpuri", res) {
  if (!is.data.frame(DATA)) {
    res$status <- 500
    return(list(error = "DATA not loaded"))
  }
  if (nchar(name) < 3 || nchar(admission) < 3) {
    res$status <- 400
    return(list(error = "Min 3 chars"))
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
# RAZORPAY WEBHOOK
# =========================================================

#* @post /razorpay/webhook
#* @serializer json
function(req, res) {
  
  message("🔥 Razorpay webhook hit")
  
  raw <- req$postBody
  sig <- req$HTTP_X_RAZORPAY_SIGNATURE
  
  if (!is.character(raw) || !is.character(sig)) {
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
  
  if (!identical(safe_get(payload, c("event")), "payment.captured")) {
    res$status <- 200
    return(list(status = "ignored"))
  }
  
  tryCatch({
    
    check_fm_env()
    
    payment <- safe_get(payload, c("payload", "payment", "entity"))
    if (!is.list(payment)) return()
    
    payment_id <- safe_get(payment, c("id"))
    if (!is.character(payment_id)) return()
    
    token <- fm_login()
    if (fm_payment_exists(token, payment_id)) {
      message("⚠️ Duplicate ignored: ", payment_id)
      return()
    }
    
    gross_amount <- num(safe_get(payment, c("amount"))) / 100
    fee          <- num(safe_get(payment, c("fee"))) / 100
    tax          <- num(safe_get(payment, c("tax"))) / 100
    net_amount   <- gross_amount - fee - tax
    
    record <- list(
      payment_id             = payment_id,
      order_id               = safe_get(payment, c("order_id")),
      currency               = safe_get(payment, c("currency")),
      `payment status`       = safe_get(payment, c("status")),
      student_name           = safe_get(payment, c("notes", "student_name")),
      admission_number       = safe_get(payment, c("notes", "admission_number")),
      branch                 = safe_get(payment, c("notes", "branch")),
      email                  = safe_get(payment, c("email")),
      phone                  = as.character(safe_get(payment, c("contact"))),
      `gross amount`         = gross_amount,
      `razorpay fee`         = fee,
      `gst on fee`           = tax,
      `net amount received`  = net_amount,
      `total payment amount` = gross_amount,
      created_via            = "webhook",
      posting_status         = "review",
      settlement_id          = ""
    )
    
    fm_insert(record)
    message("✅ Payment inserted: ", payment_id)
    
  }, error = function(e) {
    message("❌ Webhook processing error: ", e$message)
  })
  
  res$status <- 200
  list(status = "ok")
}