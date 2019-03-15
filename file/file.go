package file

import "unsafe"

/*
File is

Header
Hashes - 32 bit.
Keys - corresponding to each hash. Offset to key data
Values - corresponding to each hash
Key data

*/

type Header struct {
	NumItems  int64
	ValueSize int64
}

// Hash is the type of a hash in the table
type Hash uint32

// KeyOffset is the type of the offset to key data in the system
type KeyOffset int64

// StringLength is the type of a string length. We might not actually
// use this, but it gives us a size estimate for the string lengths
type StringLength int32

// Offsets calculates the offsets within the hash table file of the various sections within the file
func Offsets(numItems, valueSize, totalKeyLength int64) (hashes, keys, values, keyData, length int64) {

	hashes = int64(unsafe.Sizeof(Header{}))
	// Need to round this up to the next KeyOffset alignment
	keys = roundUp(hashes+int64(unsafe.Sizeof(Hash(0)))*numItems, unsafe.Alignof(KeyOffset(0)))

	// Safest to make this 8 byte aligned. Within the values the valueSize should then take care of the natural
	// alignment of the items
	values = keys + int64(unsafe.Sizeof(KeyOffset(0)))*numItems
	keyData = values + valueSize*numItems
	length = keyData + totalKeyLength + int64(unsafe.Sizeof(StringLength(0)))*numItems

	return hashes, keys, values, keyData, length
}

// roundUp increases length to the next alignment boundary required by align.
func roundUp(length int64, align uintptr) int64 {
	v := int64(align) - 1
	return (length + v) & ^(v)
}
