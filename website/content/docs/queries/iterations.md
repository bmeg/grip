---
title: Iteration
menu:
  main:
    parent: Queries
    weight: 16
---

# Iteration Commands

A common operation in graph search is the ability to iteratively repeat a search pattern. For example, a 'friend of a friend' search may become a 'friend of a friend of a friend' search. In the GripQL language cycles, iterations and conditional operations are encoded using 'mark' and 'jump' based interface. This operation is similar to using a 'goto' statement in traditional programming languages. While more primitive than the repeat mechanisms seen in Gremlin, this pattern allows for much simpler query compilation and implementation.

However, due to security concerns regarding potential denial of service attacks that could be created with the use of 'mark' and 'jump', these operations are restricted in most accounts. This is enforced by the server rejecting any queries from unauthorized users that utilize these commands without execution. In future upgrades, a proposed security feature will also allow the server to track the total number of iterations a traveler has made in a cycle and provide a hard cutoff. For example, a user could submit code with a maximum of 5 iterations.

## Operation Commands
### `.mark(name)`
Mark a segment in the stream processor, with a name, that can receive jumps. This command is used to label sections of the query operation list that can accept travelers from the `jump` command.

**Parameters:**
- `name` (str): The name given to the marked segment.

### jump(dest, condition, emit)
If a condition is true, send traveler to mark. If `emit` is True, also send a copy down the processing chain. If `condition` is None, always do the jump. This command is used to move travelers from one marked segment to another based on a specified condition.

**Parameters:**
- `dest` (str): The name of the destination mark segment. Travelers are moved to this point when their position matches the `condition` parameter.
- `condition` (_expr_ or None): An expression that determines if the traveler should jump. If it evaluates to True, the traveler jumps to the specified destination. If None, the traveler always jumps to the specified destination.
- `emit` (bool): Determines whether a copy of the traveler is emitted down the processing chain after jumping. If False, only the original traveler is processed.

### `.set(field, value)`
Set values within the traveler's memory. These values can be used to store cycle counts. This command sets a field in the traveler's memory to a specified value.

**Parameters:**
- `field` (str): The name of the field to set.
- `value` (_expr_): The value to set for the specified field. This can be any valid GripQL expression that resolves to a scalar value.

### `.increment(field, value)`
Increment a field by a specified value. This command increments a field in the traveler's memory by a specified amount.

**Parameters:**
- `field` (str): The name of the field to increment.
- `value` (_expr_): The amount to increment the specified field by. This can be any valid GripQL expression that resolves to an integer value.

## Example Queries
The following examples demonstrate how to use these commands in a query:

```python
q = G.V("Character:1").set("count", 0).as_("start").mark("a").out().increment("$start.count")
q = q.has(gripql.lt("$start.count", 2))
q = q.jump("a", None, True)
```
This query starts from a vertex with the ID "Character:1". It sets a field named "count" to 0 and annotates this vertex as "start". Then it marks this position in the operation list for future reference. The `out` command moves travelers to the outgoing edges of their current positions, incrementing the "count" field each time. If the count is less than 2, the traveler jumps back to the marked location, effectively creating a loop.

```python
q = G.V("Character:1").set("count", 0).as_("start").mark("a").out().increment("$start.count")
q = q.has(gripql.lt("$start.count", 2))
q = q.jump("a", None, False)
```
This query is similar to the previous one, but in this case, the traveler only jumps back without emitting a copy down the processing chain. The result is that only one vertex will be included in the output, even though there are multiple iterations due to the jump command.

In both examples, the use of `mark` and `jump` commands create an iterative pattern within the query operation list, effectively creating a 'friend of a friend' search that can repeat as many times as desired. These patterns are crucial for complex graph traversals in GripQL.
