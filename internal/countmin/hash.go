package countmin

// rowSalts holds one odd multiplier per row. Multiplying by an odd constant is a
// bijection on 64-bit words, and the high half of the product mixes every bit of
// the hash, so each row reads a different, independent-looking slice of the same
// hash. They were drawn at random once and are fixed, so the columns of a hash
// are stable across processes.
var rowSalts = [MaxDepth]uint64{
	0xda66bf61abbfe6fd, 0x867211215dbd27c9, 0xbf1d1079b78c23d7, 0xb13ec34222e12b4d,
	0xdef2d5bfa5641463, 0x5a88071b3f7756c3, 0xb4359964daa0e9d5, 0x9e7292f8412068e3,
	0xf27daea5d525fbb5, 0xc21a275b19059f69, 0xb564e4d16b69990f, 0xe9bd2b958b77eb01,
	0x12a47eacf9a71305, 0x4a2b1d422c845203, 0xb14557d49b17e249, 0xd9ad089ff84a66f1,
}

// shardSalt is distinct from every row salt, so the shard a hash lands in says
// nothing about its columns: keys that share a shard still spread over the whole
// width of that shard's sketch.
const shardSalt = 0x1dc0818e9a7e15bb

// column maps h onto [0, width) for the row salted by salt. The high 32 bits of
// the salted product are scaled to the width by a second multiply-shift instead
// of a modulo, which costs a division. width is at most math.MaxUint32, so the
// scaling product stays below 2^64.
func column(h, salt, width uint64) uint64 {
	return (h * salt >> 32) * width >> 32
}

// ShardOf returns which of 1<<bits shards h belongs to, always below 1<<bits. It
// uses its own salt, so it is independent of the columns h takes in a sketch.
// bits is expected to be at most 32; ShardOf returns 0 when bits is 0.
func ShardOf(h uint64, bits uint) uint64 {
	return h * shardSalt >> (64 - bits)
}
