package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"reflect"
	"syscall"
	"unsafe"

	"github.com/philpearl/aeshash"
	"github.com/philpearl/statichash/file"
)

// Table is a hash-table that can be written and extracted from a file without much setup overhead. It does
// not resize, so you need to know how many records will be written in advance. It cannot be written after
// it has been loaded from a file.
type Table struct {
	valueSize int
	numItems  int

	// This is the single allocation of all the underlying data
	arena []int64

	// These are sub-slices within arena
	hashes    []file.Hash
	keys      []file.KeyOffset
	values    []byte
	keyData   []byte
	keyOffset int

	length int64

	keyDataReader byteReader
}

// New creates a new Table for writing.
func New(numItems int, valueSize, totalKeyLength int64) *Table {

	// round up numItems to be a power of 2. This is so we can do modulo arithmetic faster
	numItems = 1 << uint(int(unsafe.Sizeof(numItems))*8-bits.LeadingZeros(uint(numItems-1)))

	hashes, keys, values, keyData, length := file.Offsets(int64(numItems), valueSize, totalKeyLength)
	t := &Table{
		valueSize: int(valueSize),
		numItems:  numItems,
	}

	// We allocate []int64 to ensure we have an 8-byte boundary for the start of our data
	t.arena = make([]int64, ((length+1)/int64(unsafe.Sizeof(int64(0))))-1)

	slice := *(*reflect.SliceHeader)(unsafe.Pointer(&t.arena))
	dataStart := slice.Data
	slice.Len = numItems
	slice.Cap = numItems

	slice.Data = dataStart + uintptr(hashes)
	t.hashes = *(*[]file.Hash)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keys)
	t.keys = *(*[]file.KeyOffset)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(values)
	slice.Len = t.numItems * t.valueSize
	slice.Cap = t.numItems * t.valueSize
	t.values = *(*[]byte)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keyData)
	slice.Len = int(length - keyData)
	slice.Cap = int(length - keyData)

	t.keyData = *(*[]byte)(unsafe.Pointer(&slice))

	return t
}

// NewFrom creates a new, fully populated table from a file.
func NewFrom(filename string) (*Table, error) {

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

	h := (*file.Header)(unsafe.Pointer(data))

	hashes, keys, values, keyData, _ := file.Offsets(h.NumItems, h.ValueSize, 0)
	t := &Table{
		valueSize: int(h.ValueSize),
		numItems:  int(h.NumItems),
	}

	dataStart := data + unsafe.Sizeof(*h)
	slice := reflect.SliceHeader{
		Len: int(h.NumItems),
		Cap: int(h.NumItems),
	}

	slice.Data = dataStart + uintptr(hashes)
	t.hashes = *(*[]file.Hash)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keys)
	t.keys = *(*[]file.KeyOffset)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(values)
	slice.Len = int(h.NumItems * h.ValueSize)
	slice.Cap = int(h.NumItems * h.ValueSize)

	t.values = *(*[]byte)(unsafe.Pointer(&slice))

	slice.Data = dataStart + uintptr(keyData)
	slice.Len = int(fileLength - keyData)
	slice.Cap = int(fileLength - keyData)
	t.keyData = *(*[]byte)(unsafe.Pointer(&slice))

	return t, nil
}

func mapMemory(fd, size uintptr) (uintptr, error) {
	data, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP,
		0, // address
		size,
		syscall.PROT_READ,
		syscall.MAP_FILE|syscall.MAP_PRIVATE,
		uintptr(fd), // No file descriptor
		0,           // offset
	)
	if errno != 0 {
		// zero errno is not nil!
		return 0, errno
	}

	return data, nil
}

// Cap returns the underlying capacity of the table
func (t *Table) Cap() int {
	return len(t.hashes)
}

// WriteTo writes the hash table to f
func (t *Table) WriteTo(f io.Writer) (int64, error) {
	h := file.Header{
		NumItems:  int64(t.numItems),
		ValueSize: int64(t.valueSize),
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

	data = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&t.arena)),
		Len:  int(t.length),
		Cap:  int(t.length),
	}))
	l2, err := f.Write(data)
	return int64(l1 + l2), err
}

// Set a key & value in the hash table
func (t *Table) Set(key string, val unsafe.Pointer) {
	hash := file.Hash(aeshash.Hash(key))

	index, found := t.find(key, hash)
	if !found {
		t.hashes[index] = hash
		t.keys[index] = t.addKey(key)
	}
	fmt.Println(len(t.values), index, t.valueSize)
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
func (t *Table) GetPtr(key string) (val unsafe.Pointer, ok bool) {
	if t == nil {
		return nil, false
	}
	hash := file.Hash(aeshash.Hash(key))
	index, found := t.find(key, hash)
	if found {
		val = unsafe.Pointer(&t.values[index*int(t.valueSize)])
	}
	return val, found
}

// find looks for the location of the key in the hash table
func (t *Table) find(key string, hashVal file.Hash) (cursor int, found bool) {
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

func (t *Table) addKey(key string) file.KeyOffset {
	start := t.keyOffset
	t.keyOffset += binary.PutVarint(t.keyData[t.keyOffset:], int64(len(key)))
	copy(t.keyData[t.keyOffset:], key)
	t.keyOffset += len(key)

	return file.KeyOffset(start)
}

func (t *Table) getKey(offset file.KeyOffset) string {
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
