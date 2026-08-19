package slidingcache

import "math/bits"

// fnv1a computes the 64-bit FNV-1a hash of s. It is allocation-free and
// deterministic, which keeps shard assignment stable and cheap on the hot path.
func fnv1a(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// roundUpPow2 returns the smallest power of two that is >= n, with a minimum of
// one. It is used to size the shard array so a bitmask can replace a modulo on
// the hot path. n is expected to be within maxShards, which Config.validate
// enforces; the bit-length form is used so that even an out-of-range n returns
// immediately instead of looping.
func roundUpPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// shardMaskOf returns the mask that maps a hash onto one of count shards, where
// count is a power of two as produced by roundUpPow2.
func shardMaskOf(count int) uint64 {
	if count <= 1 {
		return 0
	}
	return uint64(count - 1)
}
