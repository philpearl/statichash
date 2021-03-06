package statichash

import "unsafe"

/*
File is

Header
Hashes - 32 bit.
Keys - corresponding to each hash. Offset to key data
Values - corresponding to each hash
Key data

*/

type header struct {
	numItems  int64
	valueSize int64
}

// Hash is the type of a hash in the table
type hash uint32

// KeyOffset is the type of the offset to key data in the system
type keyOffset int64

// StringLength is the type of a string length. We might not actually
// use this, but it gives us a size estimate for the string lengths
type stringLength int32

// Offsets calculates the offsets within the hash table file of the various sections within the file
func offsets(numItems, valueSize, totalKeyLength int64) (hashes, keys, values, keyData, length int64) {

	hashes = int64(unsafe.Sizeof(header{}))
	// Need to round this up to the next KeyOffset alignment
	keys = roundUp(hashes+int64(unsafe.Sizeof(hash(0)))*numItems, unsafe.Alignof(keyOffset(0)))

	// Safest to make this 8 byte aligned. Within the values the valueSize should then take care of the natural
	// alignment of the items
	values = keys + int64(unsafe.Sizeof(keyOffset(0)))*numItems
	keyData = values + valueSize*numItems
	length = keyData + totalKeyLength + int64(unsafe.Sizeof(stringLength(0)))*numItems

	return hashes, keys, values, keyData, length
}

// roundUp increases length to the next alignment boundary required by align.
func roundUp(length int64, align uintptr) int64 {
	v := int64(align) - 1
	return (length + v) & ^(v)
}
