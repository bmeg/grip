package mongo

const FIELD_ID = "_id"
const FIELD_LABEL = "_label"
const FIELD_TO = "_to"
const FIELD_FROM = "_from"

const FIELD_CURRENT = "data"
const FIELD_CURRENT_ID = "data._id"
const FIELD_CURRENT_TO = "data._to"
const FIELD_CURRENT_FROM = "data._from"
const FIELD_CURRENT_LABEL = "data._label"

const FIELD_DST = "dst"
const FIELD_DST_ID = "dst._id"
const FIELD_DST_TO = "dst._to"
const FIELD_DST_FROM = "dst._from"
const FIELD_DST_LABEL = "dst._label"

func IsNodeField(f string) bool {
	return f == FIELD_ID || f == FIELD_LABEL || f == FIELD_TO || f == FIELD_FROM
}

func RemoveKeyFields(x map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range x {
		if !IsNodeField(k) {
			out[k] = v
		}
	}
	return out
}
