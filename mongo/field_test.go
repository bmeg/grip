package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {

	assert.Equal(t, ToPipelinePath("name"), "data.name")
	assert.Equal(t, ToPipelinePath("_gid"), "data._id")
	assert.Equal(t, ToPipelinePath("$a.name"), "marks.a.name")

	assert.Equal(t, ToPipelinePath("$.name"), "data.name")
	assert.Equal(t, ToPipelinePath("$"), "data")

}
