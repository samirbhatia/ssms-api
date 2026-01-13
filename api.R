library(plumber)
library(DBI)
library(duckdb)
library(jsonlite)
library(digest)
library(httr)

# =========================================================
# Environment
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
# Safe accessor
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
    body = list(query = list(list(payment_id = payment_id)), limit = 1),
    encode = "json",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  status <- status_code(res)
  if (status == 200) return(TRUE)
  if (status %in% c(401, 404, 500)) return(FALSE)
  
  message("❌ FileMaker _find unexpected status: ", status)
  FALSE
}

fm_insert_razor <- function(record) {
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
  
  if (status_code(res) == 200) return(TRUE)
  
  stop("FileMaker insert failed: ", content(res, as = "text"))
}

# =========================================================
# Razorpay Webhook
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
      
      fm_insert_razor(record)
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