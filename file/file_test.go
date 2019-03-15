package file

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestSizeofRounding(t *testing.T) {
	type lala struct {
		a int32
		b int8
	}

	assert.EqualValues(t, 8, unsafe.Sizeof(lala{}))
}

func TestAlignOf(t *testing.T) {
	assert.EqualValues(t, 8, unsafe.Alignof(time.Time{}))
	assert.EqualValues(t, 1, unsafe.Alignof(uint8(0)))
	assert.EqualValues(t, 2, unsafe.Alignof(uint16(0)))
	assert.EqualValues(t, 4, unsafe.Alignof(uint32(0)))
	assert.EqualValues(t, 8, unsafe.Alignof(uint64(0)))
}

func TestOffsets(t *testing.T) {
	type args struct {
		numItems       int64
		valueSize      int64
		totalKeyLength int64
	}
	tests := []struct {
		name        string
		args        args
		wantHashes  int64
		wantKeys    int64
		wantValues  int64
		wantKeyData int64
		wantLength  int64
	}{
		{
			name: "basic",
			args: args{
				numItems:       1,
				valueSize:      1,
				totalKeyLength: 1,
			},
			wantHashes:  16, // must be 4 byte aligned
			wantKeys:    24, // must be 8 byte aligned
			wantValues:  32, // must be 8 byte aligned
			wantKeyData: 33, // no alignment requirement
			wantLength:  38, // no alignment requirement
		},
		{
			name: "bigger",
			args: args{
				numItems:       5,
				valueSize:      17,
				totalKeyLength: 40,
			},
			wantHashes:  16,  // must be 4 byte aligned
			wantKeys:    40,  // must be 8 byte aligned
			wantValues:  80,  // must be 8 byte aligned
			wantKeyData: 165, // no alignment requirement
			wantLength:  225, // no alignment requirement
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHashes, gotKeys, gotValues, gotKeyData, gotLength := Offsets(tt.args.numItems, tt.args.valueSize, tt.args.totalKeyLength)
			if gotHashes != tt.wantHashes {
				t.Errorf("Offsets() gotHashes = %v, want %v", gotHashes, tt.wantHashes)
			}
			if gotKeys != tt.wantKeys {
				t.Errorf("Offsets() gotKeys = %v, want %v", gotKeys, tt.wantKeys)
			}
			if gotValues != tt.wantValues {
				t.Errorf("Offsets() gotValues = %v, want %v", gotValues, tt.wantValues)
			}
			if gotKeyData != tt.wantKeyData {
				t.Errorf("Offsets() gotKeyData = %v, want %v", gotKeyData, tt.wantKeyData)
			}
			if gotLength != tt.wantLength {
				t.Errorf("Offsets() gotLength = %v, want %v", gotLength, tt.wantLength)
			}
		})
	}
}

func TestRoundUp(t *testing.T) {
	type args struct {
		length int64
		align  uintptr
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			args: args{
				length: 8,
				align:  8,
			},
			want: 8,
		},
		{
			args: args{
				length: 14,
				align:  8,
			},
			want: 16,
		},
		{
			args: args{
				length: 11,
				align:  4,
			},
			want: 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := roundUp(tt.args.length, tt.args.align); got != tt.want {
				t.Errorf("roundUp() = %v, want %v", got, tt.want)
			}
		})
	}
}
