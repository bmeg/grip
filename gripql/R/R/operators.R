#' @export
eq <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "EQ"))
}

#' @export
neq <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "NEQ"))
}

#' @export
gt <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "GT"))
}

#' @export
gte <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "GTE"))
}

#' @export
lt <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "LT"))
}

#' @export
lte <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "LTE"))
}

#' @export
between <- function(lower, upper) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = c(lower, upper), "condition" = "BETWEEN"))
}

#' @export
inside <- function(lower, upper) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = c(lower, upper), "condition" = "INSIDE"))
}

#' @export
outside <- function(lower, upper) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = c(lower, upper), "condition" = "OUTSIDE"))
}

#' @export
within <- function(values) {
  check_class(k, "character")
  if (length(values) == 0) {
    values <- list()
  }
  if (length(values) == 1) {
    values <- list(values)
  }
  list("condition" = list("key" = k, "value" = values, "condition" = "WITHIN"))
}

#' @export
without <- function(values) {
  check_class(k, "character")
  if (length(values) == 0) {
    values <- list()
  }
  if (length(values) == 1) {
    values <- list(values)
  }
  list("condition" = list("key" = k, "value" = values, "condition" = "WITHOUT"))
}

#' @export
contains <- function(k, v) {
  check_class(k, "character")
  list("condition" = list("key" = k, "value" = v, "condition" = "CONTAINS"))
}
