#' @export
gripql <- function(host, user=NULL, password=NULL, token=NULL, credential_file=NULL) {
  env_vars <- Sys.getenv(c("GRIP_USER", "GRIP_PASSWORD", "GRIP_TOKEN", "GRIP_CREDENTIAL_FILE"))
  if (is.null(user)) {
    if (env_vars["GRIP_USER"] != "") {
      user <- env_vars["GRIP_USER"]
    }
  }
  if (is.null(password)) {
    if (env_vars["GRIP_PASSWORD"] != "") {
      password <- env_vars["GRIP_PASSWORD"]
    }
  }
  if (is.null(token)) {
    if (env_vars["GRIP_TOKEN"] != "") {
      token <- env_vars["GRIP_TOKEN"]
    }
  }
  if (is.null(credential_file)) {
    if (env_vars["GRIP_CREDENTIAL_FILE"] != "") {
      credential_file <- env_vars["GRIP_CREDENTIAL_FILE"]
    }
  }
  header <- list("Content-Type" = "application/json",
                 "Accept" = "application/json")
  if (!is.null(token)) {
    header["Authorization"] = sprintf("Bearer %s", token)
  } else if (!(is.null(user) || is.null(password))) {
    header["Authorization"] = sprintf("Basic %s", jsonlite::base64_enc(sprintf("%s:%s", user, password)))
  } else if (!is.null(credential_file)) {
    if (!file.exists(credential_file)) {
      stop("credential file does not exist!")
    }
    creds <- jsonlite::fromJSON(credential_file)
    creds$OauthExpires <- toString(creds$OauthExpires)
    header <- c(header, creds)
  }
  structure(list(), class = "gripql", host = host, header = header)
}

#' @export
print.gripql <- function(x) {
  print(sprintf("host: %s", attr(x, "host")))
}

#' @export
graph <- function(conn, graph) {
  check_class(conn, "gripql")
  class(conn) <- c("gripql.graph", "gripql")
  attr(conn, "graph") <- graph
  conn
}

#' @export
print.gripql.graph <- function(x) {
  print(sprintf("host: %s", attr(x, "host")))
  print(sprintf("graph: %s", attr(x, "graph")))
}

#' @export
query <- function(conn) {
  check_class(conn, "gripql.graph")
  class(conn) <- c("gripql.graph.query", "gripql.graph", "gripql")
  attr(conn, "query") <- list()
  conn
}

#' @export
print.gripql.graph.query <- function(x) {
  print(sprintf("host: %s", attr(x, "host")))
  print(sprintf("graph: %s", attr(x, "graph")))
  print(sprintf("query: %s", to_json(x)))
}

append.gripql.graph.gquery <- function(x, values, after = length(x)) {
  check_class(q, "gripql.graph.query")
  q <- attr(x, "query")
  after <- length(q)
  q[[after + 1]] <- values
  attr(x, "query") <- q
  x
}

#' @export
to_json <- function(q) {
  check_class(q, "gripql.graph.query")
  jsonlite::toJSON(attr(q, "query"), auto_unbox = T, simplifyVector = F)
}

#' @export
execute <- function(q) {
  check_class(q, "gripql.graph.query")
  body <- to_json(q)
  response <- httr::POST(url = sprintf("%s/v1/graph/%s/query", attr(q, "host"),  attr(q, "graph")),
                         body = body,
                         encode = "json",
                         httr::add_headers(unlist(attr(q, "header"), use.names = TRUE)),
                         httr::verbose())
  httr::content(response, as="text") %>%
    trimws() %>%
    strsplit("\n") %>%
    unlist() %>%
    lapply(function(x) {
        jsonlite::fromJSON(x)
    })
}

#' @export
in_ <- function(q,  labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("in" = labels))
}

#' @export
inV <- in_

#' @export
out <- function(q, labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("out" = labels))
}

#' @export
outV <- out

#' @export
both <- function(q, labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("both" = labels))
}

#' @export
inE <- function(q, labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("in_e" = labels))
}

#' @export
outE <- function(q, labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("out_e" = labels))
}

#' @export
bothE <- function(q, labels=NULL) {
  check_class(q, "gripql.graph.query")
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("both_e" = labels))
}

#' @export
has <- function(q, expression) {
  check_class(q, "gripql.graph.query")
  check_class(expression, "list")
  append.gripql.query(q, list("has" = expression))
}

#' @export
hasLabel <- function(q, label) {
  check_class(q, "gripql.graph.query")
  label <- wrap_value(label)
  names(label) <- NULL
  append.gripql.query(q, list("hasLabel" = label))
}

#' @export
hasId <- function(q, id) {
  check_class(q, "gripql.graph.query")
  id <- wrap_value(id)
  names(id) <- NULL
  append.gripql.query(q, list("hasId" = id))
}

#' @export
hasKey <- function(q, key) {
  check_class(q, "gripql.graph.query")
  key <- wrap_value(key)
  names(key) <- NULL
  append.gripql.query(q, list("hasKey" = key))
}

#' @export
fields <- function(q, fields=NULL) {
  check_class(q, "gripql.graph.query")
  fields <- wrap_value(fields)
  names(fields) <- NULL
  append.gripql.query(q, list("fields" = field))
}

#' @export
as_ <- function(q, name) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("as" = name))
}

#' @export
select <- function(q, marks) {
  check_class(q, "gripql.graph.query")
  marks <- wrap_value(marks)
  names(marks) <- NULL
  append.gripql.query(q, list("select" = list("labels" = marks)))
}

#' @export
limit <- function(q, n) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("limit" = n))
}

#' @export
skip <- function(q, n) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("skip" = n))
}

#' @export
range <- function(q, start, stop) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("range" = list("start" = start, "stop" = stop)))
}

#' @export
count <- function(q) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("count" = ""))
}

#' @export
distinct <- function(q, props=NULL) {
  check_class(q, "gripql.graph.query")
  props <- wrap_value(props)
  names(props) <- NULL
  append.gripql.query(q, list("distinct" = props))
}

#' @export
render <- function(q, template) {
  check_class(q, "gripql.graph.query")
  append.gripql.query(q, list("render" = template))
}

#' @export
aggregate <- function(q, aggregations) {
  check_class(q, "gripql.graph.query")
  aggregations <- wrap_value(aggregations)
  append.gripql.query(q, list("aggregate" = list("aggregations" = aggregations)))
}

#' @export
match <- function(q, queries) {
  check_class(q, "gripql.graph.query")
  if (length(queries) == 1) {
    queries <- list(queries)
  }
  append.gripql.query(q, list("match", list("queries" = queries)))
}
