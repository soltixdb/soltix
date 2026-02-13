package compression

// AppendVarint appends a variable-length encoded uint64 to buf
func AppendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// ReadVarint reads a variable-length encoded uint64 from data
// Returns the value and number of bytes read (0 if error)
func ReadVarint(data []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}
