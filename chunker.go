package chunker

import (
	"io"
	"sync"
)

const (
	kiB = 1024
	miB = 1024 * kiB

	// WindowSize is the size of the sliding window.
	windowSize = 64
	windowMask = windowSize - 1

	// MinSize is the default minimal size of a chunk.
	MinSize = 512 * kiB
	// MaxSize is the default maximal size of a chunk.
	MaxSize = 8 * miB

	chunkerBufSize = 512 * kiB
)

type tables struct {
	out [256]Pol
	mod [256]Pol
}

// cache precomputed tables, these are read-only anyway
var cache struct {
	entries map[Pol]tables
	sync.Mutex
}

func init() {
	cache.entries = make(map[Pol]tables)
}

type chunkerState struct {
	window [windowSize]byte
	wpos   uint64
	digest uint64

	pre   uint // wait for this many bytes before start calculating an new chunk
	count uint // used for max chunk size tracking
}

type chunkerConfig struct {
	MinSize, MaxSize uint

	pol               Pol
	polShift          uint
	tables            tables
	tablesInitialized bool
	splitmask         uint64
}

// Chunker splits content with Rabin Fingerprints.
type BaseChunker struct {
	chunkerConfig
	chunkerState
}

func NewBase(pol Pol, opts ...baseOption) *BaseChunker {
	c := &BaseChunker{
		chunkerState: chunkerState{},
		chunkerConfig: chunkerConfig{
			pol:       pol,
			MinSize:   MinSize,
			MaxSize:   MaxSize,
			splitmask: (1 << 20) - 1, // aim to create chunks of 20 bits or about 1MiB on average.
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	c.reset()
	return c
}

// Reset reinitializes the chunker with a new reader, polynomial, and options.
func (c *BaseChunker) Reset(pol Pol, opts ...baseOption) {
	*c = *NewBase(pol, opts...)
}

func (c *BaseChunker) reset() {
	c.polShift = uint(c.pol.Deg() - 8)
	c.fillTables()

	for i := 0; i < windowSize; i++ {
		c.window[i] = 0
	}

	c.digest = 0
	c.wpos = 0
	c.count = 0
	c.digest = c.slide(c.digest, 1)

	// do not start a new chunk unless at least MinSize bytes have been read
	c.pre = c.MinSize - windowSize
}

// fillTables calculates out_table and mod_table for optimization. This
// implementation uses a cache in the global variable cache.
func (c *BaseChunker) fillTables() {
	// if polynomial hasn't been specified, do not compute anything for now
	if c.pol == 0 {
		return
	}

	c.tablesInitialized = true

	// test if the tables are cached for this polynomial
	cache.Lock()
	defer cache.Unlock()
	if t, ok := cache.entries[c.pol]; ok {
		c.tables = t
		return
	}

	// calculate table for sliding out bytes. The byte to slide out is used as
	// the index for the table, the value contains the following:
	// out_table[b] = Hash(b || 0 ||        ...        || 0)
	//                          \ windowsize-1 zero bytes /
	// To slide out byte b_0 for window size w with known hash
	// H := H(b_0 || ... || b_w), it is sufficient to add out_table[b_0]:
	//    H(b_0 || ... || b_w) + H(b_0 || 0 || ... || 0)
	//  = H(b_0 + b_0 || b_1 + 0 || ... || b_w + 0)
	//  = H(    0     || b_1 || ...     || b_w)
	//
	// Afterwards a new byte can be shifted in.
	for b := 0; b < 256; b++ {
		h := Pol(b)
		for i := 1; i < windowSize; i++ {
			h = (h << 8).Mod(c.pol)
		}
		c.tables.out[b] = h
	}

	// calculate table for reduction mod Polynomial
	k := c.pol.Deg()
	for b := 0; b < 256; b++ {
		// mod_table[b] = A | B, where A = (b(x) * x^k mod pol) and  B = b(x) * x^k
		//
		// The 8 bits above deg(Polynomial) determine what happens next and so
		// these bits are used as a lookup to this table. The value is split in
		// two parts: Part A contains the result of the modulus operation, part
		// B is used to cancel out the 8 top bits so that one XOR operation is
		// enough to reduce modulo Polynomial
		c.tables.mod[b] = Pol(uint64(b)<<uint(k)).Mod(c.pol) | (Pol(b) << uint(k))
	}

	cache.entries[c.pol] = c.tables
}

// NextSplitPoint returns the index before which the buf should be split
// Returns -1 if no split point was found yet.
func (c *BaseChunker) NextSplitPoint(buf []byte) (int, uint64) {
	if !c.tablesInitialized {
		panic("tables for polynomial computation not initialized")
	}

	tab := &c.tables
	polShift := c.polShift
	// go guarantees the expected behavior for bit shifts even for shift counts
	// larger than the value width. Bounding the value of polShift allows the compiler
	// to optimize the code for 'digest >> polShift'
	if polShift > 53-8 {
		panic("the polynomial must have a degree less than or equal 53")
	}
	minSize := c.MinSize
	maxSize := c.MaxSize

	idx := 0
	// check if bytes have to be dismissed before starting a new chunk
	if c.pre > 0 {
		if c.pre >= uint(len(buf)) {
			c.pre -= uint(len(buf))
			c.count += uint(len(buf))
			return -1, 0
		}

		buf = buf[c.pre:]
		idx = int(c.pre)
		c.count += c.pre
		c.pre = 0
	}

	add := c.count
	digest := c.digest
	win := c.window
	wpos := c.wpos
	for i, b := range buf {
		out := win[wpos&windowMask]
		win[wpos&windowMask] = b
		digest ^= uint64(tab.out[out])
		wpos++

		digest = updateDigest(digest, polShift, tab, b)
		// end manual inline

		add++

		if (digest&c.splitmask) == 0 || add >= maxSize {
			if add < minSize {
				continue
			}
			c.reset()
			return idx + i + 1, digest
		}
	}
	c.digest = digest
	c.window = win
	c.wpos = wpos
	c.count += uint(len(buf))
	return -1, 0
}

func updateDigest(digest uint64, polShift uint, tab *tables, b byte) (newDigest uint64) {
	index := digest >> polShift
	digest = (digest << 8) | uint64(b)
	return digest ^ uint64(tab.mod[index])
}

func (c *BaseChunker) slide(digest uint64, b byte) (newDigest uint64) {
	out := c.window[c.wpos&windowMask]
	c.window[c.wpos&windowMask] = b
	digest ^= uint64(c.tables.out[out])
	c.wpos++

	return updateDigest(digest, c.polShift, &c.tables, b)
}

// Chunk is one content-dependent chunk of bytes whose end was cut when the
// Rabin Fingerprint had the value stored in Cut.
type Chunk struct {
	Start  uint
	Length uint
	Cut    uint64
	Data   []byte
}

type chunkerBuffer struct {
	buf  []byte
	bpos uint
	bmax uint
	pos  uint

	rd     io.Reader
	closed bool
}

// Chunker splits content with Rabin Fingerprints.
type Chunker struct {
	BaseChunker
	chunkerBuffer
}

// New returns a new Chunker based on polynomial p that reads from rd.
// Chunker behavior can be customized by passing options, see With* functions.
func New(rd io.Reader, pol Pol, opts ...option) *Chunker {
	c := &Chunker{
		BaseChunker: *NewBase(pol),
		chunkerBuffer: chunkerBuffer{
			buf: make([]byte, chunkerBufSize),
			rd:  rd,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.buf == nil {
		c.buf = make([]byte, chunkerBufSize)
	}

	c.reset()
	return c
}

// NewWithBoundaries returns a new Chunker based on polynomial p that reads from
// rd and custom min and max size boundaries.
//
// Deprecated: NewWithBoundaries uses should be replaced by New(rd, pol, WithBoundaries(min, max)).
func NewWithBoundaries(rd io.Reader, pol Pol, min, max uint) *Chunker {
	return New(rd, pol, WithBoundaries(min, max))
}

// SetAverageBits allows to control the frequency of chunk discovery:
// the lower averageBits, the higher amount of chunks will be identified.
// The default value is 20 bits, so chunks will be of 1MiB size on average.
//
// Deprecated: SetAverageBits uses should be replaced by NewBase(rd, pol, WithAverageBits(averageBits)).
func (c *Chunker) SetAverageBits(averageBits int) {
	c.splitmask = (1 << uint64(averageBits)) - 1
}

// Reset reinitializes the chunker with a new reader, polynomial, and options.
func (c *Chunker) Reset(rd io.Reader, pol Pol, opts ...option) {
	opts = append([]option{WithBuffer(c.buf)}, opts...)
	*c = *New(rd, pol, opts...)
}

// Deprecated: ResetWithBoundaries uses should be replaced by Reset(rd, pol, WithBoundaries(min, max)).
func (c *Chunker) ResetWithBoundaries(rd io.Reader, pol Pol, min, max uint) {
	c.Reset(rd, pol, WithBoundaries(min, max))
}

// Next returns the position and length of the next chunk of data. If an error
// occurs while reading, the error is returned. Afterwards, the state of the
// current chunk is undefined. When the last chunk has been returned, all
// subsequent calls yield an io.EOF error.
func (c *Chunker) Next(data []byte) (Chunk, error) {
	data = data[:0]
	start := c.pos
	for {
		if c.bpos >= c.bmax {
			n, err := io.ReadFull(c.rd, c.buf)

			if err == io.ErrUnexpectedEOF {
				err = nil
			}

			// io.ReadFull only returns io.EOF when no bytes could be read. If
			// this is the case and we're in this branch, there are no more
			// bytes to buffer, so this was the last chunk. If a different
			// error has occurred, return that error and abandon the current
			// chunk.
			if err == io.EOF && !c.closed {
				c.closed = true

				// return current chunk, if any bytes have been processed
				if len(data) > 0 {
					return Chunk{
						Start:  start,
						Length: uint(len(data)),
						// somewhat meaningless as this is not a split point
						Cut:  c.digest,
						Data: data,
					}, nil
				}
			}

			if err != nil {
				return Chunk{}, err
			}

			c.bpos = 0
			c.bmax = uint(n)
		}

		split, cut := c.NextSplitPoint(c.buf[c.bpos:c.bmax])
		if split == -1 {
			data = append(data, c.buf[c.bpos:c.bmax]...)
			c.pos += c.bmax - c.bpos
			c.bpos = c.bmax
		} else {
			data = append(data, c.buf[c.bpos:c.bpos+uint(split)]...)
			c.bpos += uint(split)
			c.pos += uint(split)

			return Chunk{
				Start:  start,
				Length: uint(len(data)),
				Cut:    cut,
				Data:   data,
			}, nil
		}
	}
}
