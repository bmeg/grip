function process(val) {
	if (!val) {
		val = []
  } else if (typeof val == "string" || typeof val == "number") {
	  val = [val]
  } else if (!Array.isArray(val)) {
		throw "not something we know how to process into an array"
	}
	return val
}

function query() {
	return {
		query: [],
		V: function(id) {
			this.query.push({'v': process(id)})
			return this
		},
		E: function(id) {
			this.query.push({'e': process(id)})
			return this
		},
		out: function(label) {
			this.query.push({'out': process(label)})
			return this
		},
		outNull: function(label) {
			this.query.push({'outNull': process(label)})
			return this
		},
		in_: function(label) {
			this.query.push({'in': process(label)})
			return this
		},
		inNull: function(label) {
			this.query.push({'inNull': process(label)})
			return this
		},
		both: function(label) {
			this.query.push({'both': process(label)})
			return this
		},
		outV: function(label) {
			this.query.push({'outV': process(label)})
			return this
		},
		inV: function(label) {
			this.query.push({'inV': process(label)})
			return this
		},
		bothV: function(label) {
			this.query.push({'bothV': process(label)})
			return this
		},
		outE: function(label) {
			this.query.push({'outE': process(label)})
			return this
		},
		outENull: function(label) {
			this.query.push({'outENull': process(label)})
			return this
		},
		inE: function(label) {
			this.query.push({'inE': process(label)})
			return this
		},
		inENull: function(label) {
			this.query.push({'inENull': process(label)})
			return this
		},
		bothE: function(label) {
			this.query.push({'bothE': process(label)})
			return this
		},
		as_: function(name) {
			this.query.push({'as': name})
			return this
		},
		select: function(name) {
			this.query.push({'select': name})
			return this
		},
		limit: function(n) {
			this.query.push({'limit': n})
			return this
		},
		skip: function(n) {
			this.query.push({'skip': n})
			return this
		},
		range: function(start, stop) {
			this.query.push({'range': {'start': start, 'stop': stop}})
			return this
		},
		count: function() {
			this.query.push({'count': ''})
			return this
		},
		distinct: function(val) {
			this.query.push({'distinct': process(val)})
			return this
		},
		fields: function(fields) {
			this.query.push({'fields': fields})
			return this
		},
		render: function(r) {
			this.query.push({'render': r})
			return this
		},
		has: function(expression) {
			this.query.push({'has': expression})
			return this
		},
		hasLabel: function(label) {
			this.query.push({'hasLabel': process(label)})
			return this
		},
		hasId: function(id) {
			this.query.push({'hasId': process(id)})
			return this
		},
		hasKey: function(key) {
			this.query.push({'hasKey': process(key)})
			return this
		},
		set: function(key, value) {
			this.query.push({'set':{'key':key, 'value':value}})
			return this
		},
		increment: function(key, value) {
			this.query.push({'increment':{'key':key, 'value':value}})
			return this
		},
		jump: function(mark, expression, emit) {
			this.query.push({"jump": {"mark":mark, "expression" : expression, "emit":emit}})
			return this
		},
		mark: function(name){
	        this.query.push({"mark": name})
			return this
		},
		aggregate: function() {
			this.query.push({'aggregate': {'aggregations': Array.prototype.slice.call(arguments)}})
			return this
		}
	}
}

// Where operators
function and_() {
	return {'and': {'expressions': Array.prototype.slice.call(arguments)}}
}

function or_() {
	return {'or': {'expressions': Array.prototype.slice.call(arguments)}}
}

function not_(expression) {
	return {'not': expression}
}

function eq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'EQ'}}
}

function neq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'NEQ'}}
}

function gt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GT'}}
}

function gte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GTE'}}
}

function lt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LT'}}
}

function lte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LTE'}}
}

function inside(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'INSIDE'}}
}

function outside(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'OUTSIDE'}}
}

function between(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'BETWEEN'}}
}

function within(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'WITHIN'}}
}

function without(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'WITHOUT'}}
}

function contains(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'CONTAINS'}}
}

// Aggregation builders
function term(name, field, size) {
	agg = {
		"name": name,
		"term": {"field": field}
	}
	if (size) {
		if (typeof size != "number") {
			throw "expected size to be a number"
		}
		agg["term"]["size"] = size
	}
	return agg
}

function percentile(name, field, percents) {
	if (!percents) {
		percents = [1, 5, 25, 50, 75, 95, 99]
	} else {
		percents = process(percents)
	}

  if (!percents.every(function(x){ return typeof x == "number" })) {
		throw "percents expected to be an array of numbers"
	}

	return {
		"name": name,
		"percentile": {
			"field": field, "percents": percents
		}
	}
}

function histogram(name, field, interval) {
	if (interval) {
		if (typeof interval != "number") {
			throw "expected interval to be a number"
		}
	}
	return {
		"name": name,
		"histogram": {
			"field": field, "interval": interval
		}
	}
}

function count(name) {
	return {
		"name": name,
		"count": {}
	}
}

function field(name, field){
    return {
        "name": name,
        "field": {
			"field": field
		}
    }
}

gripql = {
	"lt" : lt,
	"gt" : gt,
	"lte" : lte,
	"gte" : gte,
	"eq" : eq,
	"without": without,
	"within" : within,
	"inside" : inside,
	"field" : field,
	"count" : count,
	"histogram": histogram,
	"percentile": percentile,
}

function V(id) {
  return query().V(id)
}

function E(id) {
  return query().E(id)
}
