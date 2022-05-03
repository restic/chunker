package chunker

type option func(*Chunker)
type baseOption func(*BaseChunker)

// WithAverageBits allows to control the frequency of chunk discovery:
// the lower averageBits, the higher amount of chunks will be identified.
// The default value is 20 bits, so chunks will be of 1MiB size on average.
func WithBaseAverageBits(averageBits int) baseOption {
	return func(c *BaseChunker) { c.splitmask = (1 << uint64(averageBits)) - 1 }
}

// WithBoundaries allows to set custom min and max size boundaries.
func WithBaseBoundaries(min, max uint) baseOption {
	return func(c *BaseChunker) {
		c.MinSize = min
		c.MaxSize = max
	}
}

// WithAverageBits allows to control the frequency of chunk discovery:
// the lower averageBits, the higher amount of chunks will be identified.
// The default value is 20 bits, so chunks will be of 1MiB size on average.
func WithAverageBits(averageBits int) option {
	return func(c *Chunker) { c.splitmask = (1 << uint64(averageBits)) - 1 }
}

// WithBoundaries allows to set custom min and max size boundaries.
func WithBoundaries(min, max uint) option {
	return func(c *Chunker) {
		c.MinSize = min
		c.MaxSize = max
	}
}

// WithBuffer allows to set custom buffer for chunker.
func WithBuffer(buf []byte) option {
	return func(c *Chunker) { c.buf = buf }
}
