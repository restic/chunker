package chunker_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"

	"github.com/restic/chunker"
)

func ExampleChunker() {
	// generate 32MiB of deterministic pseudo-random data
	rng := rand.New(rand.NewSource(23))
	data := make([]byte, 32*1024*1024)

	_, err := rng.Read(data)
	if err != nil {
		panic(err)
	}

	// create a chunker
	chnkr := chunker.New(bytes.NewReader(data), chunker.Pol(0x3DA3358B4DC173))

	// reuse this buffer
	buf := make([]byte, 8*1024*1024)

	for i := 0; i < 5; i++ {
		chunk, err := chnkr.Next(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("%d %02x\n", chunk.Length, sha256.Sum256(chunk.Data))
	}

	// Output:
	// 1015370 615e8851030f318751f3c8baf8fbfa9958e2dd7f25dc1a87dcf6d6f79d1f1a9f
	// 1276199 f1cb038c558d3a2093049815cc45f80cd367712634a28f6dd36642f905d35c37
	// 1124437 a8e19dcd4224b58eb2b480ae42bb1a4a3b0c91c074f4745dbe3f8e4ec1a926e7
	// 3580969 2b3a3fe65ce9d689599c3b26375c40c22955bf92b170b24258e54dee91e3c2af
	// 3709129 47672502d75db244cb3dc3098eed87ffd537c9f0d66fb82a0198b6f6994409f2
}
