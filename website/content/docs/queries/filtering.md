---
title: Filtering
menu:
  main:
    parent: Queries
    weight: 4
---

# Filtering in GripQL

GripQL provides powerful filtering capabilities using the .has() method and various condition functions.
Here's a comprehensive guide:.has()The .has() method is used to filter elements (vertices or edges) based on specified conditions.

Conditions are functions provided by the gripql module that define the filtering criteria.

## Comparison Operators

### `gripql.eq(variable, value)`
Equal to (==)

```
G.V().has(gripql.eq("symbol", "TP53"))
# Returns vertices where the 'symbol' property is equal to 'TP53'.
```

### `gripql.neq(variable, value)`
Not equal to (!=)

```
G.V().has(gripql.neq("symbol", "TP53"))
# Returns vertices where the 'symbol' property is not equal to 'TP53'.
```

### `gripql.gt(variable, value)`
Greater than (>)

```
G.V().has(gripql.gt("age", 45))
# Returns vertices where the 'age' property is greater than 45.
```

### `gripql.lt(variable, value)`
Less than (<)
```
G.V().has(gripql.lt("age", 45))
# Returns vertices where the 'age' property is less than 45.
```

### `gripql.gte(variable, value)`
Greater than or equal to (>=)
```
G.V().has(gripql.gte("age", 45))
# Returns vertices where the 'age' property is greater than or equal to 45.
```

### `gripql.lte(variable, value)`
Less than or equal to (<=)

```
G.V().has(gripql.lte("age", 45))
# Returns vertices where the 'age' property is less than or equal to 45.
```

---

## Range Operators

### `gripql.inside(variable, [lower_bound, upper_bound])`
lower_bound < variable < upper_bound (exclusive)

```
G.V().has(gripql.inside("age", [30, 45]))
# Returns vertices where the 'age' property is greater than 30 and less than 45.
```

### `gripql.outside(variable, [lower_bound, upper_bound])`
variable < lower_bound OR variable > upper_bound

```
G.V().has(gripql.outside("age", [30, 45]))
# Returns vertices where the 'age' property is less than 30 or greater than 45.
```

### `gripql.between(variable, [lower_bound, upper_bound])`
lower_bound <= variable < upper_bound

```
G.V().has(gripql.between("age", [30, 45]))
# Returns vertices where the 'age' property is greater than or equal to 30 and less than 45.
```

---

## Set Membership Operators

### `gripql.within(variable, values)`
variable is in values

```
G.V().has(gripql.within("symbol", ["TP53", "BRCA1"]))
# Returns vertices where the 'symbol' property is either 'TP53' or 'BRCA1'.
```

### `gripql.without(variable, values)`
variable is not in values

```
G.V().has(gripql.without("symbol", ["TP53", "BRCA1"]))
# Returns vertices where the 'symbol' property is neither 'TP53' nor 'BRCA1'.
```

---

## String/Array Containment

### `gripql.contains(variable, value)`
The variable (which is typically a list/array) contains value.

```
G.V().has(gripql.contains("groups", "group1"))
# Returns vertices where the 'groups' property (which is a list) contains the value "group1".
# Example: {"groups": ["group1", "group2", "group3"]} would match.
```

---

## Logical Operators

### `gripql.and_([condition1, condition2, ...])`
Logical AND; all conditions must be true.

```
G.V().has(gripql.and_([gripql.lte("age", 45), gripql.gte("age", 35)]))
# Returns vertices where the 'age' property is less than or equal to 45 AND greater than or equal to 35.
```

### `gripql.or_([condition1, condition2, ...])`
Logical OR; at least one condition must be true.

```
G.V().has(gripql.or_([gripql.eq("symbol", "TP53"), gripql.eq("symbol", "BRCA1")]))
# Returns vertices where the 'symbol' property is either 'TP53' OR 'BRCA1'.
```

### `gripql.not_(condition)`
Logical NOT; negates the condition

```
G.V().has(gripql.not_(gripql.eq("symbol", "TP53")))
# Returns vertices where the 'symbol' property is NOT equal to 'TP53'.
```
