// Package statichash provides a hash-table designed to be written to file then memory-mapped in as a read-only
// table. The intention is to use it with large data tables where loading say a CSV and then hashing it has a
// considerable impact on the start-up time of the process.
//
// The table has string keys only. It cannot grow, and needs the total size of the keys as it is created. The
// expectation is that you have all the data in advance. The values should all be the same size and should not
// contain any pointers
package statichash

import (
	"encoding/binary"
	"io"
	"math/bits"
	"os"
	"reflect"
	"unsafe"

	"github.com/philpearl/aeshash"
)

// table is a hash-table that can be written and extracted from a file without much setup overhead. It does
// not resize, so you need to know how many records will be written in advance. It cannot be written after
// it has been loaded from a file.
type table struct {
	valueSize int
	numItems  int

	// This is the single allocation of all the underlying data
	arena []int64

	// These are sub-slices within arena
	hashes    []hash
	keys      []keyOffset
	values    []byte
	keyData   []byte
	keyOffset int

	length int64

	keyDataReader byteReader
}

// Write is a hash-table you can write to and save to a file. Create one via New. The intention is that you
// have the full details of the hash table before you begin, and the point is to create a hash table you can
// very quickly read from a file and use without significant initialisation.
type Write struct {
	table
}

// Read is a hash-table you can read from. The intention is that you create it from a file using NewFrom.
// Create the file using a Write
type Read struct {
	table
	data       uintptr
	dataLength uintptr
}

// New creates a new table for writing. The intention is that you know the details of the table in advance,
// including the number of items, the size of the value stored and the total length of all the key strings.
// The table must have string keys.
//
func New(numItems int, valueSize, totalKeyLength int64) *Write {

	// round up numItems to be a power of 2. This is so we can do modulo arithmetic faster
	numItems = 1 << uint(int(unsafe.Sizeof(numItems))*8-bits.LeadingZeros(uint(numItems-1)))

	hashes, keys, values, keyData, length := offsets(int64(numItems), valueSize, totalKeyLength)
	t := Write{
		table: table{
			valueSize: int(valueSize),
			numItems:  numItems,
		},
	}

	// We allocate []int64 to ensure we have an 8-byte boundary for the start of our data
	t.arena = make([]int64, ((length+1)/int64(unsafe.Sizeof(int64(0))))-1)
	t.length = length

	slice := *(*reflect.SliceHeader)(unsafe.Pointer(&t.arena))
	dataStart := slice.Data
	slice.Len = numItems
	slice.Cap = numItems

	slice.Data = dataStart + uintptr(hashes)
	t.hashes = *(*[]hash)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keys)
	t.keys = *(*[]keyOffset)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(values)
	slice.Len = t.numItems * t.valueSize
	slice.Cap = t.numItems * t.valueSize
	t.values = *(*[]byte)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keyData)
	slice.Len = int(length - keyData)
	slice.Cap = int(length - keyData)
	t.keyData = *(*[]byte)(unsafe.Pointer(&slice))

	return &t
}

// NewFrom creates a new, fully populated hash-table from a file prepared using Write.WriteTo.
func NewFrom(filename string) (*Read, error) {

	// First we map in the entire file
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	fileLength, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	data, err := mapMemory(f.Fd(), uintptr(fileLength))
	if err != nil {
		return nil, err
	}

	return newFromData(data, uintptr(fileLength))
}

// NewFromBytes creates a table from the bytes of a file saved using a Write. This can be useful if the data
// is not stored in a separate file, but rather is built into the executable via something like bindata
func NewFromBytes(data []byte) (*Read, error) {
	slice := *(*reflect.SliceHeader)(unsafe.Pointer(&data))
	return newFromData(slice.Data, uintptr(slice.Len))
}

func newFromData(data, length uintptr) (*Read, error) {
	h := (*header)(unsafe.Pointer(data))

	hashes, keys, values, keyData, _ := offsets(h.numItems, h.valueSize, 0)
	t := Read{
		table: table{
			valueSize: int(h.valueSize),
			numItems:  int(h.numItems),
		},
		data:       data,
		dataLength: length,
	}

	dataStart := data + unsafe.Sizeof(*h)
	slice := reflect.SliceHeader{
		Len: int(h.numItems),
		Cap: int(h.numItems),
	}

	slice.Data = dataStart + uintptr(hashes)
	t.hashes = *(*[]hash)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keys)
	t.keys = *(*[]keyOffset)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(values)
	slice.Len = int(h.numItems * h.valueSize)
	slice.Cap = slice.Len

	t.values = *(*[]byte)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keyData)
	slice.Len = int(int64(length) - keyData)
	slice.Cap = slice.Len
	t.keyData = *(*[]byte)(unsafe.Pointer(&slice))

	return &t, nil
}

// Close releases the resources associated with the table
func (r *Read) Close() error {
	if r.data != 0 && r.dataLength != 0 {
		if err := unmap(r.data, r.dataLength); err != nil {
			return err
		}
		r.data = 0
		r.dataLength = 0
	}

	return nil
}

// Cap returns the underlying capacity of the table
func (t *table) Cap() int {
	return len(t.hashes)
}

// WriteTo writes the hash table to f
func (t *Write) WriteTo(f io.Writer) (int64, error) {
	h := header{
		numItems:  int64(t.numItems),
		valueSize: int64(t.valueSize),
	}
	data := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&h)),
		Len:  int(unsafe.Sizeof(h)),
		Cap:  int(unsafe.Sizeof(h)),
	}))
	l1, err := f.Write(data)
	if err != nil {
		return 0, err
	}

	arenaSlice := *(*reflect.SliceHeader)(unsafe.Pointer(&t.arena))

	data = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: arenaSlice.Data,
		Len:  int(t.length),
		Cap:  int(t.length),
	}))

	l2, err := f.Write(data)
	return int64(l1 + l2), err
}

// Set a key & value in the hash table. Pass a pointer to the value. The value is copied into the hash table
// using the size passed on New. The key is also copied.
func (t *Write) Set(key string, val unsafe.Pointer) {
	hash := hash(aeshash.Hash(key))

	index, found := t.find(key, hash)
	if !found {
		t.hashes[index] = hash
		t.keys[index] = t.addKey(key)
	}
	copy(t.values[index*t.valueSize:], *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(val),
		Cap:  t.valueSize,
		Len:  t.valueSize,
	})))
}

// GetPtr gets the value associated with key. It returns an unsafe.Pointer to the value. Access this by
// casting to the appropriate type
//
//  v, ok := t.GetPtr("key")
//  if !ok {
//     return
//  }
//  value := (*myType)(v)
//
func (t *table) GetPtr(key string) (val unsafe.Pointer, ok bool) {
	if t == nil {
		return nil, false
	}
	hash := hash(aeshash.Hash(key))
	index, found := t.find(key, hash)
	if found {
		val = unsafe.Pointer(&t.values[index*int(t.valueSize)])
	}
	return val, found
}

// find looks for the location of the key in the hash table
func (t *table) find(key string, hashVal hash) (cursor int, found bool) {
	l := t.numItems
	cursor = int(hashVal) & (l - 1)
	start := cursor
	// TODO: check if 0 hash is a good indicator for an empty slot. Is hash ever zero?
	for t.hashes[cursor] != 0 {
		if t.hashes[cursor] == hashVal && t.getKey(t.keys[cursor]) == key {
			return cursor, true
		}
		cursor++
		if cursor == l {
			cursor = 0
		}
		if cursor == start {
			panic("out of space!")
		}
	}
	return cursor, false
}

// addKey saves a key. We write the length then the key bytes, and return the offset of the start of the
// length. The length is stored as a variable length int as most strings will likely be < 128 bytes
func (t *table) addKey(key string) keyOffset {
	start := t.keyOffset
	t.keyOffset += binary.PutVarint(t.keyData[t.keyOffset:], int64(len(key)))
	copy(t.keyData[t.keyOffset:], key)
	t.keyOffset += len(key)

	return keyOffset(start)
}

// getKey returns a string key.
func (t *table) getKey(offset keyOffset) string {
	t.keyDataReader.buf = t.keyData[offset:]
	t.keyDataReader.offset = 0
	len, _ := binary.ReadVarint(&t.keyDataReader)
	data := t.keyData[t.keyDataReader.offset+int(offset) : t.keyDataReader.offset+int(offset)+int(len)]
	return *(*string)(unsafe.Pointer(&data))
}

type byteReader struct {
	buf    []byte
	offset int
}

func (br *byteReader) ReadByte() (byte, error) {
	b := br.buf[br.offset]
	br.offset++
	return b, nil
}
