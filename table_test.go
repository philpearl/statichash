package statichash

import (
	"io/ioutil"
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestBasics(t *testing.T) {

	tests := []struct {
		key   string
		value int
	}{
		{"aaa", 7},
		{"aab", 6},
		{"aac", 5},
		{"aad", 4},
		{"aae", 3},
		{"aaf", 2},
		{"aag", 1},
	}

	var strLen int
	for _, test := range tests {
		strLen += len(test.key)
	}

	tb := New(len(tests), int64(unsafe.Sizeof(int(0))), int64(strLen))
	assert.Equal(t, 8, tb.Cap())

	for _, test := range tests {
		tb.Set(test.key, unsafe.Pointer(&test.value))
	}

	// Note we don't really expect to read from the map as we write it - only after loading from file
	for _, test := range tests {
		valptr, ok := tb.GetPtr(test.key)
		assert.True(t, ok)
		assert.Equal(t, test.value, *(*int)(valptr))
	}
}

func TestOverWrite(t *testing.T) {
	tb := New(10, int64(unsafe.Sizeof(int(0))), 30)
	var val int
	val = 1
	tb.Set("heelo", unsafe.Pointer(&val))
	val = 42
	tb.Set("heelo", unsafe.Pointer(&val))
	val = 100

	out, ok := tb.GetPtr("heelo")
	assert.True(t, ok)
	assert.Equal(t, 42, *(*int)(out))
}

func TestWriteRead(t *testing.T) {
	tests := []struct {
		key   string
		value int
	}{
		{"aaa", 7},
		{"aab", 6},
		{"aac", 5},
		{"aad", 4},
		{"aae", 3},
		{"aaf", 2},
		{"aag", 1},
	}

	var strLen int
	for _, test := range tests {
		strLen += len(test.key)
	}

	tb := New(len(tests), int64(unsafe.Sizeof(int(0))), int64(strLen))
	assert.Equal(t, 8, tb.Cap())

	for _, test := range tests {
		tb.Set(test.key, unsafe.Pointer(&test.value))
	}

	f, err := ioutil.TempFile("", "")
	assert.NoError(t, err)
	defer f.Close()
	defer os.Remove(f.Name())
	_, err = tb.WriteTo(f)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	tr, err := NewFrom(f.Name())
	assert.NoError(t, err)
	defer tr.Close()
	assert.Equal(t, 8, tr.Cap())

	// Note we don't really expect to read from the map as we write it - only after loading from file
	for _, test := range tests {
		valptr, ok := tr.GetPtr(test.key)
		if assert.True(t, ok) {
			assert.Equal(t, test.value, *(*int)(valptr))
		}
	}
}
