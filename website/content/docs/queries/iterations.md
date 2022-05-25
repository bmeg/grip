---
title: Iteration
menu:
  main:
    parent: Queries
    weight: 10
---

# Iteration API

A common operation in graph search is the ability to iterative repeat a search pattern.
For example, a 'friend of a friend' search may become a 'friend of a friend of a friend' search.

In the GripQL language cycles, iterations and conditional operations are encoded using 
'mark' and 'jump' based interface. This operations are similar to using a 'goto' 
statement in a traditional programming language. While more primitive than the 
repeat mechanisms seen in Gremlin, this pattern allows for a much more simple 
query compilation and implementation.

Because the mark and jump pattern can be abused for denial of service attacks, 
ie to create infinite loops, the authorization model will allow this particular set
of operations to be turned off for some accounts. This means that queries from 
unauthorized users that utilize the 'mark' or 'jump' commands will be rejected 
by the server without execution. One additional proposed security feature, that 
is being discussed for in future upgrades, will allow the server to 
track the total number of iterations a traveler has made in a cycle, and provide a 
hard cutoff. For example, a user could submit code with a maximum of 5 iterations.

## Operation Commands
### mark(name)
Mark a segment in the stream processor, with a name, that can receive jumps

### jump(dest, condition, emit)
If a condition is true, send traveler to mark. If emit is true, also send a 
copy down the processing chain. If condition is None, always do the jump.


### set(field, value)
Set values within the travelers memory. These values be used to store cycle counts

### increment(field, value)
Increment field, ie data[field] = data[field] + value. Can be used to increment counter every cycle


## Example queries

```
q = G.query().V("Character:1").set("count", 0).as_("start").mark("a").out().increment("$start.count")
q = q.has(gripql.lt("$start.count", 2))
q = q.jump("a", None, True)
```

