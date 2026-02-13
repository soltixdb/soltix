package compression

import "math/bits"

// BitWriter writes individual bits to a byte buffer.
// Used by Gorilla XOR bit-packing encoder.
type BitWriter struct {
	buf     []byte
	current byte  // current byte being filled
	bitPos  uint8 // bits written in current byte (0-7)
}

// NewBitWriter creates a BitWriter with the given initial capacity.
func NewBitWriter(capacity int) *BitWriter {
	return &BitWriter{
		buf: make([]byte, 0, capacity),
	}
}

// WriteBit writes a single bit (0 or 1).
func (w *BitWriter) WriteBit(bit byte) {
	if bit != 0 {
		w.current |= 1 << (7 - w.bitPos)
	}
	w.bitPos++
	if w.bitPos == 8 {
		w.buf = append(w.buf, w.current)
		w.current = 0
		w.bitPos = 0
	}
}

// WriteBits writes the lowest `nbits` bits of val (MSB first).
// nbits must be <= 64.
func (w *BitWriter) WriteBits(val uint64, nbits uint8) {
	// Fast path: fill current byte, then write whole bytes, then remainder.
	remaining := nbits
	for remaining > 0 {
		bitsAvail := 8 - w.bitPos // bits available in current byte
		if remaining >= bitsAvail {
			// Fill the rest of the current byte
			shift := remaining - bitsAvail
			w.current |= byte(val >> shift)
			// Mask to only keep the lower `remaining - bitsAvail` bits
			if shift < 64 {
				val &= (1 << shift) - 1
			}
			remaining -= bitsAvail
			w.buf = append(w.buf, w.current)
			w.current = 0
			w.bitPos = 0
		} else {
			// Remaining bits fit in current byte
			shift := bitsAvail - remaining
			w.current |= byte(val << shift)
			w.bitPos += remaining
			remaining = 0
		}
	}
}

// Bytes returns the final byte buffer, flushing any partial byte.
func (w *BitWriter) Bytes() []byte {
	if w.bitPos > 0 {
		return append(w.buf, w.current)
	}
	return w.buf
}

// BitLen returns total number of bits written.
func (w *BitWriter) BitLen() int {
	return len(w.buf)*8 + int(w.bitPos)
}

// -----------------------------------------------------------------------

// BitReader reads individual bits from a byte buffer.
// Used by Gorilla XOR bit-packing decoder.
type BitReader struct {
	data    []byte
	byteOff int   // current byte offset
	bitOff  uint8 // current bit offset within byte (0-7)
}

// NewBitReader creates a BitReader over the given data.
func NewBitReader(data []byte) *BitReader {
	return &BitReader{data: data}
}

// ReadBit reads a single bit. Returns 0 or 1 and ok=false if exhausted.
func (r *BitReader) ReadBit() (byte, bool) {
	if r.byteOff >= len(r.data) {
		return 0, false
	}
	bit := (r.data[r.byteOff] >> (7 - r.bitOff)) & 1
	r.bitOff++
	if r.bitOff == 8 {
		r.bitOff = 0
		r.byteOff++
	}
	return bit, true
}

// ReadBits reads `nbits` bits and returns them right-aligned in a uint64.
// nbits must be <= 64. Returns ok=false if not enough data.
func (r *BitReader) ReadBits(nbits uint8) (uint64, bool) {
	var val uint64
	remaining := nbits
	for remaining > 0 {
		if r.byteOff >= len(r.data) {
			return 0, false
		}
		bitsAvail := 8 - r.bitOff // bits left in current byte
		if remaining >= bitsAvail {
			// Take all remaining bits from current byte
			mask := byte((1 << bitsAvail) - 1)
			val = (val << bitsAvail) | uint64(r.data[r.byteOff]&mask)
			remaining -= bitsAvail
			r.bitOff = 0
			r.byteOff++
		} else {
			// Take only `remaining` bits from current byte (MSB side)
			shift := bitsAvail - remaining
			mask := byte((1 << remaining) - 1)
			val = (val << remaining) | uint64((r.data[r.byteOff]>>shift)&mask)
			r.bitOff += remaining
			remaining = 0
		}
	}
	return val, true
}

// LeadingZeros64 counts leading zeros in a 64-bit value.
func LeadingZeros64(x uint64) uint8 {
	return uint8(bits.LeadingZeros64(x))
}

// TrailingZeros64 counts trailing zeros in a 64-bit value.
func TrailingZeros64(x uint64) uint8 {
	return uint8(bits.TrailingZeros64(x))
}
