package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtils_GetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(i)))
	}
}

func TestUtils_RandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		// t.Log(string(RandomValue(10)))
		assert.NotNil(t, string(RandomValue(10)))
	}
}
