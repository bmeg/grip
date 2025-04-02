package gripql

func (qr *QueryResult) ToInterface() any {
	out := map[string]any{}
	if v := qr.GetVertex(); v != nil {
		out["vertex"] = v.GetDataMap()
	}
	if e := qr.GetEdge(); e != nil {
		out["edge"] = e.GetDataMap()
	}
	if c := qr.GetCount(); c != 0 {
		out["count"] = c
	}
	if r := qr.GetRender(); r != nil {
		out["render"] = r.AsInterface()
	}
	if a := qr.GetAggregations(); a != nil {
		out["aggregation"] = map[string]any{
			"key":   a.GetKey().AsInterface(),
			"value": a.GetValue(),
			"name":  a.GetName(),
		}
	}
	if p := qr.GetPath(); p != nil {
		pa := []any{}
		for _, c := range p.GetValues() {
			pa = append(pa, c.AsInterface())
		}
		out["path"] = pa
	}
	return out
}
