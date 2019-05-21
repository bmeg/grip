package gen3

import (
	"testing"
)

func TestLoadSchema(t *testing.T) {
	path := "./example-json-schemas/experiment.yaml"
	s, err := loadSchema(path)
	if err != nil {
		t.Error(err)
	}
	if s.ID != "experiment" {
		t.Logf("%+v", s.ID)
		t.Errorf("unexpected schema ID")
	}
	if len(s.Links) != 1 {
		t.Logf("%+v", s.Links)
		t.Errorf("unexpected number of Links")
	}
}

func TestLoadSchemas(t *testing.T) {
	path := "./example-json-schemas"
	s, err := loadAllSchemas(path)
	if err != nil {
		t.Error(err)
	}
	if len(s) != 4 {
		t.Logf("%+v", s)
		t.Errorf("unexpected number of vertices in loaded json schema")
	}
}
