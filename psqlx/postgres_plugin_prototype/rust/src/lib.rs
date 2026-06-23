use pgrx::prelude::*;
use pgrx::JsonB;
use serde::Deserialize;
use serde_json::{json, Map, Value};

pgrx::pg_module_magic!();

#[derive(Debug, Deserialize)]
struct QuerySet {
    query: Vec<Value>,
}

fn sample_vertices() -> Vec<Value> {
    vec![
        json!({"id":"v1","label":"Person","data":{"name":"Alice","age":30}}),
        json!({"id":"v2","label":"Person","data":{"name":"Bob","age":25}}),
        json!({"id":"v3","label":"City","data":{"name":"Paris"}}),
    ]
}

fn sample_edges() -> Vec<Value> {
    vec![
        json!({"id":"e1","label":"knows","from":"v1","to":"v2","data":{"since":2020}}),
        json!({"id":"e2","label":"lives_in","from":"v1","to":"v3","data":{}}),
    ]
}

fn sample_vertex_by_id(id: &str) -> Option<Value> {
    sample_vertices()
        .into_iter()
        .find(|vertex| vertex.get("id").and_then(Value::as_str) == Some(id))
}

fn as_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect(),
        Some(Value::String(item)) => vec![item.clone()],
        _ => vec![],
    }
}

fn to_vertex_row(vertex: &Value) -> Value {
    json!({
        "result_type": "vertex",
        "vertex": vertex,
    })
}

fn to_edge_row(edge: &Value) -> Value {
    json!({
        "result_type": "edge",
        "edge": edge,
    })
}

fn to_count_row(count: usize) -> Value {
    json!({
        "result_type": "count",
        "count": count,
    })
}

fn to_render_row(graph: &str, current: &[Value], step_count: usize) -> Value {
    json!({
        "result_type": "render",
        "render": {
            "graph": graph,
            "step_count": step_count,
            "rows": current,
        }
    })
}

fn vertex_id(vertex: &Value) -> Option<&str> {
    vertex.get("id").and_then(Value::as_str)
}

fn vertex_label(vertex: &Value) -> Option<&str> {
    vertex.get("label").and_then(Value::as_str)
}

fn vertex_data(vertex: &Value) -> Option<&Map<String, Value>> {
    vertex.get("data").and_then(Value::as_object)
}

fn filter_vertices_by_ids(current: Vec<Value>, ids: &[String]) -> Vec<Value> {
    if ids.is_empty() {
        return current;
    }
    current
        .into_iter()
        .filter(|vertex| vertex_id(vertex).map(|id| ids.iter().any(|needle| needle == id)).unwrap_or(false))
        .collect()
}

fn filter_vertices_by_label(current: Vec<Value>, labels: &[String]) -> Vec<Value> {
    if labels.is_empty() {
        return current;
    }
    current
        .into_iter()
        .filter(|vertex| vertex_label(vertex).map(|label| labels.iter().any(|needle| needle == label)).unwrap_or(false))
        .collect()
}

fn filter_vertices_by_keys(current: Vec<Value>, keys: &[String]) -> Vec<Value> {
    if keys.is_empty() {
        return current;
    }
    current
        .into_iter()
        .filter(|vertex| {
            vertex_data(vertex)
                .map(|data| keys.iter().all(|key| data.contains_key(key)))
                .unwrap_or(false)
        })
        .collect()
}

fn outgoing_vertices(current: &[Value], labels: &[String]) -> Vec<Value> {
    let edges = sample_edges();
    let mut out = Vec::new();
    for vertex in current {
        let Some(id) = vertex_id(vertex) else {
            continue;
        };
        for edge in &edges {
            let edge_from = edge.get("from").and_then(Value::as_str);
            let edge_label = edge.get("label").and_then(Value::as_str);
            if edge_from == Some(id)
                && (labels.is_empty() || edge_label.map(|label| labels.iter().any(|needle| needle == label)).unwrap_or(false))
            {
                if let Some(target_id) = edge.get("to").and_then(Value::as_str) {
                    if let Some(target) = sample_vertex_by_id(target_id) {
                        out.push(target);
                    }
                }
            }
        }
    }
    out
}

fn both_vertices(current: &[Value], labels: &[String]) -> Vec<Value> {
    let mut out = outgoing_vertices(current, labels);
    out.extend(incoming_vertices(current, labels));
    out
}

fn incoming_vertices(current: &[Value], labels: &[String]) -> Vec<Value> {
    let edges = sample_edges();
    let mut out = Vec::new();
    for vertex in current {
        let Some(id) = vertex_id(vertex) else {
            continue;
        };
        for edge in &edges {
            let edge_to = edge.get("to").and_then(Value::as_str);
            let edge_label = edge.get("label").and_then(Value::as_str);
            if edge_to == Some(id)
                && (labels.is_empty() || edge_label.map(|label| labels.iter().any(|needle| needle == label)).unwrap_or(false))
            {
                if let Some(source_id) = edge.get("from").and_then(Value::as_str) {
                    if let Some(source) = sample_vertex_by_id(source_id) {
                        out.push(source);
                    }
                }
            }
        }
    }
    out
}

fn outgoing_edges(current: &[Value], labels: &[String]) -> Vec<Value> {
    let edges = sample_edges();
    let mut out = Vec::new();
    for vertex in current {
        let Some(id) = vertex_id(vertex) else {
            continue;
        };
        for edge in &edges {
            let edge_from = edge.get("from").and_then(Value::as_str);
            let edge_label = edge.get("label").and_then(Value::as_str);
            if edge_from == Some(id)
                && (labels.is_empty() || edge_label.map(|label| labels.iter().any(|needle| needle == label)).unwrap_or(false))
            {
                out.push(edge.clone());
            }
        }
    }
    out
}

fn incoming_edges(current: &[Value], labels: &[String]) -> Vec<Value> {
    let edges = sample_edges();
    let mut out = Vec::new();
    for vertex in current {
        let Some(id) = vertex_id(vertex) else {
            continue;
        };
        for edge in &edges {
            let edge_to = edge.get("to").and_then(Value::as_str);
            let edge_label = edge.get("label").and_then(Value::as_str);
            if edge_to == Some(id)
                && (labels.is_empty() || edge_label.map(|label| labels.iter().any(|needle| needle == label)).unwrap_or(false))
            {
                out.push(edge.clone());
            }
        }
    }
    out
}

fn both_edges(current: &[Value], labels: &[String]) -> Vec<Value> {
    let mut out = outgoing_edges(current, labels);
    out.extend(incoming_edges(current, labels));
    out
}

fn rows_to_jsonb_rows(rows: Vec<Value>) -> Vec<(JsonB,)> {
    rows.into_iter().map(|row| (JsonB(row),)).collect()
}

fn evaluate_query(graph: &str, query: &QuerySet) -> Result<Vec<Value>, String> {
    let mut current = sample_vertices();
    let mut step_count = 0usize;

    for stmt in &query.query {
        step_count += 1;
        let obj = stmt
            .as_object()
            .ok_or_else(|| "each query statement must be a JSON object".to_string())?;

        if let Some(v) = obj.get("v") {
            let ids = as_string_list(Some(v));
            current = if ids.is_empty() {
                sample_vertices()
            } else {
                filter_vertices_by_ids(sample_vertices(), &ids)
            };
            continue;
        }

        if let Some(v) = obj.get("has_label").or_else(|| obj.get("hasLabel")) {
            let labels = as_string_list(Some(v));
            current = filter_vertices_by_label(current, &labels);
            continue;
        }

        if let Some(v) = obj.get("has_id").or_else(|| obj.get("hasId")) {
            let ids = as_string_list(Some(v));
            current = filter_vertices_by_ids(current, &ids);
            continue;
        }

        if let Some(v) = obj.get("has_key").or_else(|| obj.get("hasKey")) {
            let keys = as_string_list(Some(v));
            current = filter_vertices_by_keys(current, &keys);
            continue;
        }

        if let Some(v) = obj.get("out") {
            let labels = as_string_list(Some(v));
            current = outgoing_vertices(&current, &labels);
            continue;
        }

        if let Some(v) = obj.get("both") {
            let labels = as_string_list(Some(v));
            current = both_vertices(&current, &labels);
            continue;
        }

        if let Some(v) = obj.get("in") {
            let labels = as_string_list(Some(v));
            current = incoming_vertices(&current, &labels);
            continue;
        }

        if let Some(v) = obj.get("out_e").or_else(|| obj.get("outE")) {
            let labels = as_string_list(Some(v));
            let edges = outgoing_edges(&current, &labels);
            return Ok(edges.into_iter().map(|edge| to_edge_row(&edge)).collect());
        }

        if let Some(v) = obj.get("both_e").or_else(|| obj.get("bothE")) {
            let labels = as_string_list(Some(v));
            let mut edges = both_edges(&current, &labels);
            return Ok(edges.drain(..).map(|edge| to_edge_row(&edge)).collect());
        }

        if let Some(v) = obj.get("in_e").or_else(|| obj.get("inE")) {
            let labels = as_string_list(Some(v));
            let edges = incoming_edges(&current, &labels);
            return Ok(edges.into_iter().map(|edge| to_edge_row(&edge)).collect());
        }

        if let Some(v) = obj.get("limit") {
            let limit = v.as_u64().ok_or_else(|| "limit must be a non-negative integer".to_string())? as usize;
            current.truncate(limit);
            continue;
        }

        if obj.contains_key("count") {
            return Ok(vec![to_count_row(current.len())]);
        }

        if let Some(render) = obj.get("render") {
            return Ok(vec![to_render_row(graph, &current, step_count).merge_render(render)]);
        }

        return Err(format!("unsupported statement in prototype: {obj:?}"));
    }

    Ok(current.into_iter().map(|vertex| to_vertex_row(&vertex)).collect())
}

trait RenderMerge {
    fn merge_render(self, requested: &Value) -> Value;
}

impl RenderMerge for Value {
    fn merge_render(self, requested: &Value) -> Value {
        match self {
            Value::Object(mut map) => {
                map.insert("requested".to_string(), requested.clone());
                Value::Object(map)
            }
            other => json!({
                "result_type": "render",
                "render": {
                    "requested": requested,
                    "value": other,
                }
            }),
        }
    }
}

fn grip_exec_rows(graph: &str, query: JsonB) -> Vec<(JsonB,)> {
    let parsed: Result<QuerySet, _> = serde_json::from_value(query.0.clone());

    let rows = match parsed {
        Ok(qs) => match evaluate_query(graph, &qs) {
            Ok(rows) => rows,
            Err(err) => vec![json!({
                "result_type": "error",
                "error": {
                    "code": "UNSUPPORTED_QUERY",
                    "message": err,
                }
            })],
        },
        Err(err) => vec![json!({
            "result_type": "error",
            "error": {
                "code": "INVALID_QUERY_PAYLOAD",
                "message": err.to_string(),
            }
        })],
    };

    rows_to_jsonb_rows(rows)
}

fn grip_ping_message() -> &'static str {
    "grip_ext ready"
}

#[pg_schema]
mod grip_ext {
    use super::*;

    #[pg_extern]
    fn grip_exec(graph: &str, query: JsonB) -> TableIterator<'static, (name!(row, JsonB),)> {
        TableIterator::new(super::grip_exec_rows(graph, query).into_iter())
    }

    #[pg_extern]
    fn grip_ping() -> &'static str {
        super::grip_ping_message()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_grip_exec_returns_rows() {
        let payload = JsonB(json!({"query": [{"v": []}, {"count": {}}]}));
        let rows = grip_exec_rows("test-graph", payload);
        assert!(!rows.is_empty());
    }

    #[pg_test]
    fn test_grip_exec_can_filter_and_count() {
        let payload = JsonB(json!({"query": [{"v": []}, {"has_label": ["Person"]}, {"count": {}}]}));
        let rows = grip_exec_rows("test-graph", payload);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.0["result_type"], "count");
    }

    #[pg_test]
    fn test_grip_exec_can_traverse_out() {
        let payload = JsonB(json!({"query": [{"v": ["v1"]}, {"out": ["knows"]}]}));
        let rows = grip_exec_rows("test-graph", payload);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.0["result_type"], "vertex");
        assert_eq!(rows[0].0.0["vertex"]["id"], "v2");
    }

    #[pg_test]
    fn test_grip_ping() {
        assert_eq!(grip_ping_message(), "grip_ext ready");
    }
}
