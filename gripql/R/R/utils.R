check_class <- function(x, class) {
  if (!inherits(x, class)) {
    stop(sprintf("expected object of type %s, but got %s", class, class(x)))
  }
}

wrap_value <- function(v) {
  if (is.list(v) || length(v) > 1) {
    return(v)
  } else if (is.null(v)) {
    return(list())
  } else {
    return(list(v))
  }
} 
