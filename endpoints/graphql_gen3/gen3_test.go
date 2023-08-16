package main

//grip query synthea 'V().hasLabel("DocumentReference").out("subject")'
/*documentReference (filter:$filter) {
  subject{
    id
  }
}*/
import (
	"bytes"
	"encoding/json"
	"net/http"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func HTTP_REQUEST(graph_name string, url string, payload []byte, t *testing.T) (response_json map[string]any, status bool) {
	req, err := http.NewRequest("POST", url+graph_name, bytes.NewBuffer(payload))
	if err != nil {
		t.Error("Error creating request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Error("Error sending request:", err)
		return nil, false
	}
	defer resp.Body.Close()

	t.Log("Response Status:", resp.Status)

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		t.Error("Error reading response:", err)
		return nil, false
	}

	var data map[string]interface{}
	errors := json.Unmarshal([]byte(buf.String()), &data)
	t.Log("DATA: ", data)
	if errors != nil {
		t.Error("Error:", errors)
		return nil, false
	}
	return data, true
}
func Test_Filters(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "Slider and CheckBox"},
		{name: "Aggregation and Filter"},
		{name: "Combo_test"},
		{name: "NullOps"},
		{name: "GraphQL_NullOps"},
		{name: "NullOP_Results"},
	}
	for _, tt := range tests {
		if tt.name == "Slider and CheckBox" {
			t.Run(tt.name, func(t *testing.T) {
				payload := []byte(`{
					"query": "query ($filter: JSON) {\n  patient(filter: $filter) {\n    quality_adjusted_life_years_valueDecimal\n    maritalStatus\n  }\n}\n",
					"variables": {
					  "filter": {
						"AND": [
						  {
							"AND": [
							  {
								">=": {
								  "quality_adjusted_life_years_valueDecimal": 66
								}
							  },
							  {
								"<=": {
								  "quality_adjusted_life_years_valueDecimal": 70
								}
							  }
							]
						  },
						  {
							"IN": {
							  "maritalStatus": [
								"M"
							  ]
							}
						  }
						]
					  }
					}
				  }`)
				data, status := HTTP_REQUEST("synthea", "http://localhost:8201/api/graphql/", payload, t)
				if status == false {
					t.Error("HTTP Request failed")
				}
				if data, ok := data["data"].(map[string]any); ok {
					if data, ok := data["patient"]; ok {
						if data, ok := data.([]any); ok {
							for _, value := range data {
								if data, ok := value.(map[string]any); ok {
									if upper_bound, ok := data["quality_adjusted_life_years_valueDecimal"].(float64); ok {
										if data["maritalStatus"] == "M" && upper_bound >= 66 && upper_bound <= 70 {
											continue
										} else {
											t.Error("Row Has failed filter")
										}
									} else {
										t.Error("Row has failed type check")
									}
								}
							}
						}
					}
				}

			})
		}
		if tt.name == "Aggregation and Filter" {
			payload := []byte(`{
				"query": "query ($filter: JSON) {\n  _aggregation {\n  observation (filter: $filter) {\n    code {\n      histogram {\n        count\n        key\n      }\n    }\n\n  }}\n}\n",
				"variables": {
				  "filter": {
					"AND": [
					  {
						"IN": {
						  "code": [
							"Creatinine"
						  ]
						}
					  }
					]
				  }
				}
			  }`)
			data, status := HTTP_REQUEST("synthea", "http://localhost:8201/api/graphql/", payload, t)
			if status == false {
				t.Error("test failed")
			}
			if data, ok := data["data"].(map[string]any)["_aggregation"].(map[string]any)["observation"].(map[string]any)["code"].(map[string]any)["histogram"].([]any); ok {
				for _, values := range data {
					key := values.(map[string]any)["key"].(string)
					count := values.(map[string]any)["count"].(float64)
					if key != `string_value:"Creatinine"` || count != 5377 {
						t.Error("Aggregation test failed. Did data change?")
					}
				}
			} else {
				t.Error("indexing failed. Did query change?")
			}
		}
		if tt.name == "Combo_test" {
			payload := []byte(`{
				"query": "query ($filter: JSON) {\n  _aggregation {\n  documentReference {\n    category {\n      histogram {\n        count\n        key\n      }\n    }\n  }\n  }\n  observation(filter: $filter) {\n    subject\n  }\n}\n",
				"variables": {
				  "filter": {
					"AND": [
					  {
						"IN": {
						  "subject": [
							"Patient/5b13b8fc-f387-4a95-bb80-5c22eeed7697"
						  ]
						}
					  }
					]
				  }
				}
			  }`)
			data, status := HTTP_REQUEST("synthea", "http://localhost:8201/api/graphql/", payload, t)
			if status == false {
				t.Error("test failed on HTTP Request")
			}
			if data, ok := data["data"].(map[string]any); ok {
				if aggregation, ok := data["_aggregation"].(map[string]any)["documentReference"].(map[string]any)["category"].(map[string]any)["histogram"].([]any); ok {
					for i, values := range aggregation {
						if map_values, ok := values.(map[string]any); ok {
							t.Log("MAP VALUES: ", map_values)
							switch i {
							case 0:
								if map_values["key"].(string) == `string_value:"Clinical Note"` && map_values["count"].(float64) == 37378 {
									continue
								} else {
									t.Error("test failed, values don't match")
								}
							case 1:
								if map_values["key"].(string) == `string_value:"Image"` && map_values["count"].(float64) == 125 {
									continue
								} else {
									t.Error("test failed, values don't match")
								}
							case 2:
								if map_values["key"].(string) == `string_value:"Cancer related multigene analysis Molgen Doc (cfDNA)"` && map_values["count"].(float64) == 9 {
									continue
								} else {
									t.Error("test failed, values don't match")
								}
							}
						}
					}
					if res, ok := data["observation"].([]any); ok {
						for _, val := range res {
							t.Log("INFO: ", val)
							if val.(map[string]any)["subject"] != "Patient/5b13b8fc-f387-4a95-bb80-5c22eeed7697" {
								t.Error("filter test failed, values don't match")
							}
						}
					}
				}

			}

		}
		if tt.name == "NullOps" {
			cmd := exec.Command("grip", "query", "outNull", `V(["875e3325-c2ad-4d63-82ad-be8432bd415b","1842609e-7a40-4ba3-8a82-2fa061fcf30f"]).outNull("subject_Patient")`)
			output, err := cmd.Output()

			if err != nil {
				t.Error("Error:", err)
			}

			json_string := strings.Split(string(output), "\n")
			var jsonMap map[string]interface{}
			var NullOp map[string]interface{}

			json.Unmarshal([]byte(json_string[0]), &jsonMap)
			json.Unmarshal([]byte(json_string[1]), &NullOp)

			if val, ok := NullOp["vertex"]; ok {
				if val == nil || reflect.DeepEqual(val, reflect.Zero(reflect.TypeOf(val)).Interface()) {
					t.Error("Null Op Test Failed", val)
				}
			}

			//t.Log("NULL OP", NullOp)
		}
		if tt.name == "GraphQL_NullOps" {
			payload := []byte(`{
				"query": "query ($filter: JSON) {\n  documentReference (filter:$filter, first: 1) {\n    file_name\n    subject {\n      id\n      birthDate\n      subject_observation {\n        code\n      }\n    }   \n  }\n}\n",
				"variables": {}
			  }`)
			data, status := HTTP_REQUEST("synthea", "http://localhost:8201/api/graphql/", payload, t)
			if status == false {
				t.Error("test failed on HTTP Request")
			}
			if data, ok := data["data"].(map[string]any)["documentReference"].([]any); ok {
				if data, ok := data[0].(map[string]any); ok {
					t.Log("CHECK: ", data["file_name"] == "output/clinical_reports/53fefa32-fcbb-4ff8-8a92-55ee120877b7")
					if data["file_name"] != "output/clinical_reports/53fefa32-fcbb-4ff8-8a92-55ee120877b7" {
						t.Error()
					}
					if data, ok := data["subject"].([]any); ok {
						if data, ok := data[0].(map[string]any); ok {
							t.Log("CHECK: ", data["birthDate"] == "1913-10-29" || data["id"] != "fb60e763-e799-4d59-82a3-66977cc6696c")
							if data["birthDate"] != "1913-10-29" || data["id"] != "fb60e763-e799-4d59-82a3-66977cc6696c" {
								t.Error()
							}
							if data, ok := data["subject_observation"].([]any); ok {
								if data, ok := data[0].(map[string]any); ok {
									t.Log("CHECK: ", data["code"] == "Bilirubin.total [Mass/volume] in Serum or Plasma")
									if data["code"] != "Bilirubin.total [Mass/volume] in Serum or Plasma" {
										t.Error()
									}

								}
							}
						}
					}
				}
			}
		}
		if tt.name == "NullOP_Results" {
			payload := []byte(`{
				"query": "query ($filter: JSON) {\n  documentReference (filter:$filter, first: 7) {\n    file_name\n    subject {\n      id\n      birthDate\n      subject_observation {\n        code\n      }\n    }   \n  }\n}\n",
				"variables": {}
			  }`)
			data, status := HTTP_REQUEST("synthea", "http://localhost:8201/api/graphql/", payload, t)
			if status == false {
				t.Error("test failed on HTTP Request")
			}
			if data, ok := data["data"].(map[string]any)["documentReference"].([]any); ok {
				if len(data) != 7 {
					t.Error("Unexpected output length")
				}
			} else {
				t.Error("Unexpected output structure")
			}
		}
	}
}
