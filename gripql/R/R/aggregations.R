#' @export
term <- function(name, field, size=NULL) {
  agg <- list(
    "name" = name,
    "term" = list("field" = field)
  )
  if (!is.null(size)) {
    agg["term"]["size"] = size
  }
  return(agg)
}

#' @export
percentile <- function(name, field, percents=c(1, 5, 25, 50, 75, 95, 99)) {
  agg <- list(
    "name" = name,
    "percentile" = list("field" = field,
                        "percents" = percents)
  )
  return(agg)
}

#' @export
histogram <- function(name, field, interval) {
  agg <- list(
    "name" = name,
    "percentile" = list("field" = field,
                        "interval" = interval)
  )
  return(agg)
}
