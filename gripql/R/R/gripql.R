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
    header["Authorization"] = paste("Bearer", token, sep=" ")
  } else if (!(is.null(user) | is.null(password))) {
    header["Authorization"] = paste("Basic", jsonlite::base64_enc(paste(user, password, sep = ":"), sep = " ")
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
  print(paste("host:", attr(x, "host"), sep = " "))
}

#' @export
graph.gripql <- function(conn, graph) {
  class(conn) <- c("gripql.graph", "gripql")
  attr(conn, "graph") <- graph
  conn
}

#' @export
query.gripql.graph <- function(conn) {
  class(conn) <- c("gripql.graph.query", "gripql.graph", "gripql")
  attr(conn, "query") <- list()
  conn
}

#' @export
print.gripql.graph.query <- function(x) {
  print(attr(x, "query"))
}

append.gripql.graph.gquery <- function(x, values, after = length(x)) {
  q <- attr(x, "query")
  after <- length(q)
  q[[after + 1]] <- values
  attr(x, "query") <- q
  x
}

#' @export
to_json.gripql.graph.query <- function(q) {
  jsonlite::toJSON(attr(q, "query"), auto_unbox = T, simplifyVector = F)
}

#' @export
execute.gripql.graph.query <- function(q) {
  body <- to_json(q)
  response <- httr::POST(url = paste(attr(q, "host"), "/v1/graph/", attr(q, "graph"), "/query", sep = ""),
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

wrap_value <- function(v) {
  if (is.list(v) || length(v) > 1) {
    return(v)
  } else if (is.null(v)) {
    return(list())
  } else {
    return(list(v))
  }
} 

#' @export
in_ <- function(q,  labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("in" = labels))
}

#' @export
inV <- in_

#' @export
out <- function(q, labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("out" = labels))
}

#' @export
outV <- out

#' @export
both <- function(q, labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("both" = labels))
}

#' @export
inE <- function(q, labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("in_e" = labels))
}

#' @export
outE <- function(q, labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("out_e" = labels))
}

#' @export
bothE <- function(q, labels=NULL) {
  labels <- wrap_value(labels)
  names(labels) <- NULL
  append.gripql.query(q, list("both_e" = labels))
}

#' @export
has <- function(q, expression) {
  append.gripql.query(q, list("has" = expression))
}

#' @export
hasLabel <- function(q, label) {
  label <- wrap_value(label)
  names(label) <- NULL
  append.gripql.query(q, list("hasLabel" = label))
}

#' @export
hasId <- function(q, id) {
  id <- wrap_value(id)
  names(id) <- NULL
  append.gripql.query(q, list("hasId" = id))
}

#' @export
hasKey <- function(q, key) {
  key <- wrap_value(key)
  names(key) <- NULL
  append.gripql.query(q, list("hasKey" = key))
}

#' @export
fields <- function(q, fields=NULL) {
  fields <- wrap_value(fields)
  names(fields) <- NULL
  append.gripql.query(q, list("fields" = field))
}

#' @export
as_ <- function(q, name) {
  append.gripql.query(q, list("as" = name))
}

#' @export
select <- function(q, marks) {
  marks <- wrap_value(marks)
  names(marks) <- NULL
  append.gripql.query(q, list("select" = list("labels" = marks)))
}

#' @export
limit <- function(q, n) {
  append.gripql.query(q, list("limit" = n))
}

#' @export
skip <- function(q, n) {
  append.gripql.query(q, list("skip" = n))
}

#' @export
range <- function(q, start, stop) {
  append.gripql.query(q, list("range" = list("start" = start, "stop" = stop)))
}

#' @export
count <- function(q) {
  append.gripql.query(q, list("count" = ""))
}

#' @export
distinct <- function(q, props=NULL) {
  props <- wrap_value(props)
  names(props) <- NULL
  append.gripql.query(q, list("distinct" = props))
}

#' @export
render <- function(q, template) {
  append.gripql.query(q, list("render" = template))
}

#' @export
aggregate <- function(q, aggregations) {
  aggregations <- wrap_value(aggregations)
  append.gripql.query(q, list("aggregate" = list("aggregations" = aggregations)))
}

#' @export
match <- function(q, queries) {
  if (length(queries) == 1) {
    queries <- list(queries)
  }
  append.gripql.query(q, list("match", list("queries" = queries)))
}
