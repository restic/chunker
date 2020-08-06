package chunker

import (
	"io"
	"math/rand"
)

const (
	kiB = 1024
	miB = 1024 * kiB
	// MinSize is the default minimal size of a chunk.
	MinSize = 512 * kiB
	// MaxSize is the default maximal size of a chunk.
	MaxSize = 8 * miB
	//chunk Buffer Size is 2 * MinSize
	chunkerBufSize = 2 * MinSize
)

var  MaskArray = [...]uint64{
	0, 0, 0, 0, 0, 0,
	0x00001803110, 			// 64B
	0x000018035100, 		// 128B
	0x00001800035300, 		// 256B
	0x000019000353000,		// 512B
	0x0000590003530000,		// 1KB
	0x0000d90003530000,		// 2KB
	0x0000d90103530000,		// 4KB
	0x0000d90303530000,		// 8KB
	0x0000d90313530000,		// 16KB
	0x0000d90f03530000,		// 32KB
	0x0000d90303537000,		// 64KB
	0x0000d90703537000,		// 128KB
	0x0000d90707537000,	 	// 256KB
	0x0000d91707537000,		// 512KB
	0x0000d91747537000,		// 1MB
	0x0000d91767537000,		// 2MB
	0x0000d93767537000, 	// 4MB
	0x0000d93777537000, 	// 8MB
	0x0000d93777577000,		// 16MB
}

// table is a pre-calculate random data array
// it is used to generate chunk hash result
// it will make xor operation with a seed to make a random array.
var table = [256]uint64{
	0xe80e8d55032474b3, 0x11b25b61f5924e15, 0x03aa5bd82a9eb669, 0xc45a153ef107a38c,
	0xeac874b86f0f57b9, 0xa5ccedec95ec79c7, 0xe15a3320ad42ac0a, 0x5ed3583fa63cec15,
	0xcd497bf624a4451d, 0xf9ade5b059683605, 0x773940c03fb11ca1, 0xa36b16e4a6ae15b2,
	0x67afd1adb5a89eac, 0xc44c75ee32f0038e, 0x2101790f365c0967, 0x76415c64a222fc4a,
	0x579929249a1e577a, 0xe4762fc41fdbf750, 0xea52198e57dfcdcc, 0xe2535aafe30b4281,
	0xcb1a1bd6c77c9056, 0x5a1aa9bfc4612a62, 0x15a728aef8943eb5, 0x2f8f09738a8ec8d9,
	0x200f3dec9fac8074, 0x0fa9a7b1e0d318df, 0x06c0804ffd0d8e3a, 0x630cbc412669dd25,
	0x10e34f85f4b10285, 0x2a6fe8164b9b6410, 0xcacb57d857d55810, 0x77f8a3a36ff11b46,
	0x66af517e0dc3003e, 0x76c073c789b4009a, 0x853230dbb529f22a, 0x1e9e9c09a1f77e56,
	0x1e871223802ee65d, 0x37fe4588718ff813, 0x10088539f30db464, 0x366f7470b80b72d1,
	0x33f2634d9a6b31db, 0xd43917751d69ea18, 0xa0f492bc1aa7b8de, 0x3f94e5a8054edd20,
	0xedfd6e25eb8b1dbf, 0x759517a54f196a56, 0xe81d5006ec7b6b17, 0x8dd8385fa894a6b7,
	0x45f4d5467b0d6f91, 0xa1f894699de22bc8, 0x33829d09ef93e0fe, 0x3e29e250caed603c,
	0xf7382cba7f63a45e, 0x970f95412bb569d1, 0xc7fcea456d356b4b, 0x723042513f3e7a57,
	0x17ae7688de3596f1, 0x27ac1fcd7cd23c1a, 0xf429beeb78b3f71f, 0xd0780692fb93a3f9,
	0x9f507e28a7c9842f, 0x56001ad536e433ae, 0x7e1dd1ecf58be306, 0x15fee353aa233fc6,
	0xb033a0730b7638e8, 0xeb593ad6bd2406d1, 0x7c86502574d0f133, 0xce3b008d4ccb4be7,
	0xf8566e3d383594c8, 0xb2c261e9b7af4429, 0xf685e7e253799dbb, 0x05d33ed60a494cbc,
	0xeaf88d55a4cb0d1a, 0x3ee9368a902415a1, 0x8980fe6a8493a9a4, 0x358ed008cb448631,
	0xd0cb7e37b46824b8, 0xe9bc375c0bc94f84, 0xea0bf1d8e6b55bb3, 0xb66a60d0f9f6f297,
	0x66db2cc4807b3758, 0x7e4e014afbca8b4d, 0xa5686a4938b0c730, 0xa5f0d7353d623316,
	0x26e38c349242d5e8, 0xeeefa80a29858e30, 0x8915cb912aa67386, 0x4b957a47bfc420d4,
	0xbb53d051a895f7e1, 0x09f5e3235f6911ce, 0x416b98e695cfb7ce, 0x97a08183344c5c86,
	0xbf68e0791839a861, 0xea05dde59ed3ed56, 0x0ca732280beda160, 0xac748ed62fe7f4e2,
	0xc686da075cf6e151, 0xe1ba5658f4af05c8, 0xe9ff09fbeb67cc35, 0xafaea9470323b28d,
	0x0291e8db5bb0ac2a, 0x342072a9bbee77ae, 0x03147eed6b3d0a9c, 0x21379d4de31dbadb,
	0x2388d965226fb986, 0x52c96988bfebabfa, 0xa6fc29896595bc2d, 0x38fa4af70aa46b8b,
	0xa688dd13939421ee, 0x99d5275d9b1415da, 0x453d31bb4fe73631, 0xde51debc1fbe3356,
	0x75a3c847a06c622f, 0xe80e32755d272579, 0x5444052250d8ec0d, 0x8f17dfda19580a3b,
	0xf6b3e9363a185e42, 0x7a42adec6868732f, 0x32cb6a07629203a2, 0x1eca8957defe56d9,
	0x9fa85e4bc78ff9ed, 0x20ff07224a499ca7, 0x3fa6295ff9682c70, 0xe3d5b1e3ce993eff,
	0xa341209362e0b79a, 0x64bd9eae5712ffe8, 0xceebb537babbd12a, 0x5586ef404315954f,
	0x46c3085c938ab51a, 0xa82ccb9199907cee, 0x8c51b6690a3523c8, 0xc4dbd4c9ae518332,
	0x979898dbb23db7b2, 0x1b5b585e6f672a9d, 0xce284da7c4903810, 0x841166e8bb5f1c4f,
	0xb7d884a3fceca7d0, 0xa76468f5a4572374, 0xc10c45f49ee9513d, 0x68f9a5663c1908c9,
	0x0095a13476a6339d, 0xd1d7516ffbe9c679, 0xfd94ab0c9726f938, 0x627468bbdb27c959,
	0xedc3f8988e4a8c9a, 0x58efd33f0dfaa499, 0x21e37d7e2ef4ac8b, 0x297f9ab5586259c6,
	0xda3ba4dc6cb9617d, 0xae11d8d9de2284d2, 0xcfeed88cb3729865, 0xefc2f9e4f03e2633,
	0x8226393e8f0855a4, 0xd6e25fd7acf3a767, 0x435784c3bfd6d14a, 0xf97142e6343fe757,
	0xd73b9fe826352f85, 0x6c3ac444b5b2bd76, 0xd8e88f3e9fd4a3fd, 0x31e50875c36f3460,
	0xa824f1bf88cf4d44, 0x54a4d2c8f5f25899, 0xbff254637ce3b1e6, 0xa02cfe92561b3caa,
	0x7bedb4edee9f0af7, 0x879c0620ac49a102, 0xa12c4ccd23b332e7, 0x09a5ff47bf94ed1e,
	0x7b62f43cd3046fa0, 0xaa3af0476b9c2fb9, 0x22e55301abebba8e, 0x3a6035c42747bd58,
	0x1705373106c8ec07, 0xb1f660de828d0628, 0x065fe82d89ca563d, 0xf555c2d8074d516d,
	0x6bb6c186b423ee99, 0x54a807be6f3120a8, 0x8a3c7fe2f88860b8, 0xbeffc344f5118e81,
	0xd686e80b7d1bd268, 0x661aef4ef5e5e88b, 0x5bf256c654cd1dda, 0x9adb1ab85d7640f4,
	0x68449238920833a2, 0x843279f4cebcb044, 0xc8710cdefa93f7bb, 0x236943294538f3e6,
	0x80d7d136c486d0b4, 0x61653956b28851d3, 0x3f843be9a9a956b5, 0xf73cfbbf137987e5,
	0xcf0cb6dee8ceac2c, 0x50c401f52f185cae, 0xbdbe89ce735c4c1c, 0xeef3ade9c0570bc7,
	0xbe8b066f8f64cbf6, 0x5238d6131705dcb9, 0x20219086c950e9f6, 0x634468d9ed74de02,
	0x0aba4b3d705c7fa5, 0x3374416f725a6672, 0xe7378bdf7beb3bc6, 0x0f7b6a1b1cee565b,
	0x234e4c41b0c33e64, 0x4efa9a0c3f21fe28, 0x1167fc551643e514, 0x9f81a69d3eb01fa4,
	0xdb75c22b12306ed0, 0xe25055d738fc9686, 0x9f9f167a3f8507bb, 0x195f8336d3fbe4d3,
	0x8442b6feffdcb6f6, 0x1e07ed24746ffde9, 0x140e31462d555266, 0x8bd0ce515ae1406e,
	0x2c0be0042b5584b3, 0x35a23d0e15d45a60, 0xc14f1ba147d9bc83, 0xbbf168691264b23f,
	0xad2cc7b57e589ade, 0x9501963154c7815c, 0x9664afa6b8d67d47, 0x7f9e5101fea0a81c,
	0x45ecffb610d25bfd, 0x3157f7aecf9b6ab3, 0xc43ca6f88d87501d, 0x9576ff838dee38dc,
	0x93f21afe0ce1c7d7, 0xceac699df343d8f9, 0x2fec49e29f03398d, 0x8805ccd5730281ed,
	0xf9fc16fc750a8e59, 0x35308cc771adf736, 0x4a57b7c9ee2b7def, 0x03a4c6cdc937a02a,
	0x6c9a8a269fc8c4fc, 0x4681decec7a03f43, 0x342eecded1353ef9, 0x8be0552d8413a867,
	0xc7b4ac51beda8be8, 0xebcc64fb719842c0, 0xde8e4c7fb6d40c1c, 0xcc8263b62f9738b1,
	0xd3cfc0f86511929a, 0x466024ce8bb226ea, 0x459ff690253a3c18, 0x98b27e9d91284c9c,
	0x75c3ae8aa3af373d, 0xfbf8f8e79a866ffc, 0x32327f59d0662799, 0x8228b57e729e9830,
	0x065ceb7a18381b58, 0xd2177671a31dc5ff, 0x90cd801f2f8701f9, 0x9d714428471c65fe,
}

// Chunk is one content-dependent chunk of bytes whose end was cut when the
// Rabin Fingerprint had the value stored in Cut.
type Chunk struct {
	Start  uint
	Length uint
	Cut    uint64
	Data   []byte
}

type chunkerState struct {
	buf  []byte
	bpos uint
	bmax uint

	start uint
	count uint
	pos   uint

	pre uint // wait for this many bytes before start calculating an new chunk

	digest uint64
}

type chunkerConfig struct {
	MinSize, MaxSize uint
	splitmask         uint64
    splitmask2        uint64

	rd     io.Reader
	closed bool
}

// Chunker splits content with Rabin Fingerprints.
type Chunker struct {
	chunkerConfig
	chunkerState
}

// SetAverageBits allows to control the frequency of chunk discovery:
// the lower averageBits, the higher amount of chunks will be identified.
// The default value is 20 bits, so chunks will be of 1MiB size on average.
func (c *Chunker) SetAverageBits(averageBits int) {
	c.splitmask = (1 << uint64(averageBits)) - 1
}

// New returns a new Chunker based on polynomial p that reads from rd.
func New(rd io.Reader, pol Pol) *Chunker {
	//return NewWithBoundaries(rd, pol, MinSize, MaxSize)

	var seed = uint64(rand.Uint32()) << 32 + uint64(rand.Uint32())

	for i:= 0; i < len(table); i++ {
		table[i] = table[i] ^ seed
	}

	return NewWithBoundaries(rd, pol, MinSize, MaxSize)
}

// NewWithBoundaries returns a new Chunker based on the two pre-defined split masks
//and reads from rd and custom min and max size boundaries.
func NewWithBoundaries(rd io.Reader, pol Pol, min, max uint) *Chunker {
	c := &Chunker{
		chunkerState: chunkerState{
			buf: make([]byte, chunkerBufSize),
		},
		chunkerConfig: chunkerConfig{
			//pol:       pol,
			rd:        rd,
			MinSize:   min,
			MaxSize:   max,
			// average chunk size is 1 MiB, so set the splitMask is 512KiB and 2 MiB
            splitmask: MaskArray[19],
            splitmask2: MaskArray[21],
		},
	}

	c.reset()

	return c
}

// Reset reinitializes the chunker with a new reader and polynomial.
func (c *Chunker) Reset(rd io.Reader, pol Pol) {
	c.ResetWithBoundaries(rd, pol, MinSize, MaxSize)
}

// ResetWithBoundaries reinitializes the chunker with a new reader,
// and custom min and max size boundaries.
func (c *Chunker) ResetWithBoundaries(rd io.Reader, pol Pol, min, max uint) {
	*c = Chunker{
		chunkerState: chunkerState{
			buf: c.buf,
		},
		chunkerConfig: chunkerConfig{
			//pol:       pol,
			rd:        rd,
			MinSize:   min,
			MaxSize:   max,
			//splitmask: (1 << 20) - 1,
            splitmask: MaskArray[19],
			splitmask2: MaskArray[21],
		},
	}

	c.reset()
}

func (c *Chunker) reset() {
	c.closed = false
	c.count = 0
	c.start = c.pos

	// do not start a new chunk unless at least MinSize bytes have been read
	c.pre = c.MinSize
}

// Next returns the position and length of the next chunk of data. If an error
// occurs while reading, the error is returned. Afterwards, the state of the
// current chunk is undefined. When the last chunk has been returned, all
// subsequent calls yield an io.EOF error.
func (c *Chunker) Next(data []byte) (Chunk, error) {
	data = data[:0]

	// go guarantees the expected behavior for bit shifts even for shift counts
	// larger than the value width. Bounding the value of polShift allows the compiler
	// to optimize the code for 'digest >> polShift'

	minSize := c.MinSize
	maxSize := c.MaxSize
	buf := c.buf

	for {
		if c.bpos >= c.bmax {
			n, err := io.ReadFull(c.rd, buf[:])

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
				if c.count > 0 {
					return Chunk{
						Start:  c.start,
						Length: c.count,
						Cut:    c.digest,
						Data:   data,
					}, nil
				}
			}

			if err != nil {
				return Chunk{}, err
			}

			c.bpos = 0
			c.bmax = uint(n)
		}

		// check if bytes have to be dismissed before starting a new chunk
		if c.pre > 0 {
			n := c.bmax - c.bpos
			if c.pre > uint(n) {
				c.pre -= uint(n)
				data = append(data, buf[c.bpos:c.bmax]...)

				c.count += uint(n)
				c.pos += uint(n)
				c.bpos = c.bmax

				continue
			}

			data = append(data, buf[c.bpos:c.bpos+c.pre]...)

			c.bpos += c.pre
			c.count += c.pre
			c.pos += c.pre
			c.pre = 0
		}

		add := c.count

        n := c.bmax - c.bpos
		mid := minSize + 4 * 1024
		if n > maxSize {
			n = maxSize
		}else if n < mid{
			mid = n
		}

		// calculate fp when loop idx from bpos to n
		// when fp matches the pre-defined splitmask
		// it generate a cut point
		// when idx reaches n and still not matches mask, it stores the data.
		// if idx reaches maxSize, it generate a cut point directly to limit the chunk size.

		idx := c.bpos
		var fp uint64 = 0
		for ; idx < mid; idx++  {
			fp = (fp << 1) + table[buf[idx]]
			add ++
			if fp & c.splitmask == 0 {
				i := add - c.count - 1
				data = append(data, c.buf[c.bpos:c.bpos + i + 1]...)
				c.count = add
				c.pos += uint(i) + 1
				c.bpos += uint(i) + 1
				c.buf = buf
				chunk := Chunk {
					Start: c.start,
					Length: c.count,
					Cut: fp,
					Data: data,
				}
				c.reset()
				return chunk, nil
			}
		}

		for ; idx < n; idx ++ {
			fp = (fp << 1) + table[buf[idx]]
			add ++
			if fp & c.splitmask2 == 0 {
				i := add - c.count - 1
				data = append(data, c.buf[c.bpos: c.bpos + i + 1]...)
				c.count = add
				c.pos += uint(i) + 1
				c.bpos += uint(i) + 1
				c.buf = buf

				chunk := Chunk {
					Start: c.start,
					Length: c.count,
					Cut: fp,
					Data: data,
				}
				c.reset()
				return chunk, nil
			}
		}

		if idx >= maxSize {
			i := add - c.count - 1
			data = append(data, c.buf[c.bpos: c.bpos+i+1]...)
			c.count = add
			c.pos += uint(i) + 1
			c.bpos += uint(i) + 1
			c.buf = buf

			chunk := Chunk {
				Start: c.start,
				Length: c.count,
				Cut: fp,
				Data: data,
			}
			c.reset()
			return chunk, nil
		}


		steps := c.bmax - c.bpos
		if steps > 0 {
			data = append(data, c.buf[c.bpos:c.bpos+steps]...)
		}
		c.count += steps
		c.pos += steps
		c.bpos = c.bmax
	}
}
