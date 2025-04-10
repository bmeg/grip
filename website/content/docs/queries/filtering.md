---
title: Filtering
menu:
  main:
    parent: Queries
    weight: 4
---


# Filtering
## .has()
Filter elements using conditional statements

```python
G.query().V().has(gripql.eq("_label", "Gene")).has(gripql.eq("symbol", "TP53"))
```

## Conditions
Conditions are arguments to `.has()` that define selection conditions

### gripql.eq(variable, value)
Returns rows where variable == value
```python
.has(gripql.eq("symbol", "TP53"))
```

---

### gripql.neq(variable, value)
Returns rows where variable != value
```python
.has(gripql.neq("symbol", "TP53"))
```

---

### gripql.gt(variable, value)
Returns rows where variable > value
```python
.has(gripql.gt("age", 45))
```

---

### gripql.lt(variable, value)
Returns rows where variable < value
```python
.has(gripql.lt("age", 45))
```

---

### gripql.gte(variable, value)
Returns rows where variable >= value
```python
.has(gripql.gte("age", 45))
```

---

### gripql.lte(variable, value)
Returns rows where variable <= value
```python
.has(gripql.lte("age", 45))
```

---

### gripql.inside(variable, [lower_bound, upper_bound])
Returns rows where variable > lower_bound && variable < upper_bound
```python
.has(gripql.inside("age", [30, 45]))
```

---

### gripql.outside(variable, [lower_bound, upper_bound])
Returns rows where variable < lower_bound || variable > upper_bound
```python
.has(gripql.outside("age", [30, 45]))
```

---

### gripql.between(variable, [lower_bound, upper_bound])
Returns rows where variable >= lower_bound && variable < upper_bound
```python
.has(gripql.between("age", [30, 45]))
```

---

### gripql.within(variable, value)
Returns rows where variable is within provided values
```python
.has(gripql.within("symbol", ["TP53", "BRCA1"]))
```

---

### gripql.without(variable, value)
Returns rows where variable is not within provided values
```python
.has(gripql.within("symbol", ["TP53", "BRCA1"]))
```

---

### gripql.contains(variable, value)
Returns rows where variable contains value
```python
.has(gripql.in_("groups", "group1"))
```

An example returned record
```
{"groups" : ["group1", "group2"]}
```

---

### gripql.and_([conditions])
```python
.has(gripql.and_( [gripql.lte("age", 45), gripql.gte("age", 35)] ))
```

---

### gripql.or_([conditions])
```python
.has(gripql.or_( [...] ))
```

---

### gripql.not_(condition)
```python
.has(gripql.not_( [...] ))
```
