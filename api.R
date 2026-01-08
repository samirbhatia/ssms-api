library(plumber)
library(DBI)
library(duckdb)
library(jsonlite)
library(digest)
library(httr)

# =========================================================
# Environment
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
    body = list(),
    encode = "json",
    config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
  )
  
  httr::stop_for_status(res)
  .fm_token <<- content(res)$response$token
  .fm_token
}

fm_payment_exists <- function(token, payment_id) {
  
  res <- tryCatch({
    httr::POST(
      paste0(
        FM_HOST,
        "/fmi/data/vLatest/databases/",
        FM_FILE,
        "/layouts/razor/_find"
      ),
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
  }, error = function(e) {
    stop("❌ FileMaker find request failed: ", e$message)
  })
  
  status <- httr::status_code(res)
  
  # ✅ Record exists
  if (status == 200) {
    return(TRUE)
  }
  
  # ✅ No records found (THIS IS NORMAL)
  if (status == 401) {
    return(FALSE)
  }
  
  # ❌ Anything else is a real error
  stop(
    "❌ FileMaker find failed: ",
    httr::content(res, as = "text")
  )
}

fm_insert_razor <- function(token, record) {
  res <- httr::POST(
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
  
  httr::stop_for_status(res)
  invisible(TRUE)
}

# =========================================================
# Razorpay Webhook
# =========================================================

#* @post /razorpay/webhook
#* @serializer json
function(req, res) {
  
  message("🔥 Razorpay webhook hit")
  
  sig <- req$HTTP_X_RAZORPAY_SIGNATURE
  raw_body <- req$postBody
  
  if (is.null(sig) || is.null(raw_body) || raw_body == "") {
    res$status <- 400
    return(list(error = "Invalid webhook"))
  }
  
  if (!verify_razorpay_signature(raw_body, sig)) {
    res$status <- 401
    return(list(error = "Invalid signature"))
  }
  
  payload <- fromJSON(raw_body, simplifyVector = FALSE)
  
  if (payload$event != "payment.captured") {
    return(list(status = "ignored"))
  }
  
  tryCatch({
    
    payment <- payload$payload$payment$entity
    token <- fm_login()
    
    if (!fm_payment_exists(token, payment$id)) {
      
      record <- list(
        payment_id = payment$id,
        order_id = payment$order_id,
        `total payment amount` = payment$amount / 100,
        currency = payment$currency,
        `payment status` = payment$status,
        student_name = payment$notes$student_name,
        admission_number = payment$notes$admission_number,
        branch = payment$notes$branch,
        email = payment$email,
        phone = payment$contact
      )
      
      fm_insert_razor(token, record)
      message("✅ Payment inserted: ", payment$id)
    }
    
  }, error = function(e) {
    message("❌ Webhook processing error: ", e$message)
  })
  
  res$status <- 200
  list(status = "ok")
}