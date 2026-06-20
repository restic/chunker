package chunker

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"strconv"
)

// Pol is a polynomial from F_2[X].
type Pol uint64

// Add returns x+y.
func (x Pol) Add(y Pol) Pol {
	r := Pol(uint64(x) ^ uint64(y))
	return r
}

// Mul returns x*y. When an overflow occurs, Mul panics.
func (x Pol) Mul(y Pol) (p Pol) {
	if x == 0 || y == 0 {
		return
	}

	if y&(y-1) == 0 {
		if x.Deg()+y.Deg() >= 64 {
			panic("multiplication would overflow uint64")
		}
		return x << uint(y.Deg())
	}

	for i := 0; i <= y.Deg(); i++ {
		if (y & (1 << uint(i))) != 0 {
			p = p.Add(x << uint(i))
		}
	}

	if p.Div(y) != x {
		panic("multiplication would overflow uint64")
	}

	return p
}

// Deg returns the degree of the polynomial x. If x is zero, -1 is returned.
func (x Pol) Deg() int {
	return bits.Len64(uint64(x)) - 1
}

// String returns the coefficients in hex.
func (x Pol) String() string {
	return "0x" + strconv.FormatUint(uint64(x), 16)
}

// Expand returns the string representation of the polynomial x.
func (x Pol) Expand() string {
	if x == 0 {
		return "0"
	}

	s := ""
	for i := x.Deg(); i > 1; i-- {
		if x&(1<<uint(i)) > 0 {
			s += fmt.Sprintf("+x^%d", i)
		}
	}

	if x&2 > 0 {
		s += "+x"
	}

	if x&1 > 0 {
		s += "+1"
	}

	return s[1:]
}

// DivMod returns x / d = q, and remainder r,
// see https://en.wikipedia.org/wiki/Division_algorithm
func (x Pol) DivMod(d Pol) (q Pol, r Pol) {
	if x == 0 {
		return q, r
	}

	if d == 0 {
		panic("division by zero")
	}

	r = x
	D := d.Deg()
	diff := x.Deg() - D
	if diff < 0 {
		return q, r
	}

	for diff >= 0 {
		m := d << uint(diff)
		q |= 1 << uint(diff)
		r = r.Add(m)

		diff = r.Deg() - D
	}

	return q, r
}

// Div returns the integer division result x / d.
func (x Pol) Div(d Pol) Pol {
	q, _ := x.DivMod(d)
	return q
}

// Mod returns the remainder of x / d
func (x Pol) Mod(d Pol) Pol {
	_, r := x.DivMod(d)
	return r
}

// I really dislike having a function that does not terminate, so specify a
// really large upper bound for finding a new irreducible polynomial, and
// return an error when no irreducible polynomial has been found within
// randPolMaxTries.
const randPolMaxTries = 1e6

// RandomPolynomial returns a new random irreducible polynomial
// of degree 53 using the default System CSPRNG as source.
// It is equivalent to calling DerivePolynomial(rand.Reader).
func RandomPolynomial() (Pol, error) {
	return DerivePolynomial(rand.Reader)
}

// DerivePolynomial returns an irreducible polynomial of degree 53
// (largest prime number below 64-8) by reading bytes from source.
// There are (2^53-2/53) irreducible polynomials of degree 53 in
// F_2[X], c.f. Michael O. Rabin (1981): "Fingerprinting by Random
// Polynomials", page 4. If no polynomial could be found in one
// million tries, an error is returned.
func DerivePolynomial(source io.Reader) (Pol, error) {
	for i := 0; i < randPolMaxTries; i++ {
		var f Pol

		// choose polynomial at (pseudo)random
		err := binary.Read(source, binary.LittleEndian, &f)
		if err != nil {
			return 0, err
		}

		// mask away bits above bit 53
		f &= Pol((1 << 54) - 1)

		// set highest and lowest bit so that the degree is 53 and the
		// polynomial is not trivially reducible
		f |= (1 << 53) | 1

		// test if f is irreducible
		if f.Irreducible() {
			return f, nil
		}
	}

	// If this is reached, we haven't found an irreducible polynomial in
	// randPolMaxTries. This error is very unlikely to occur.
	return 0, errors.New("unable to find new random irreducible polynomial")
}

// GCD computes the Greatest Common Divisor x and f.
func (x Pol) GCD(f Pol) Pol {
	if f == 0 {
		return x
	}

	return f.GCD(x.Mod(f))
}

// Irreducible returns true iff x is irreducible over F_2. This function
// uses Ben Or's reducibility test.
//
// For details see "Tests and Constructions of Irreducible Polynomials over
// Finite Fields".
func (x Pol) Irreducible() bool {
	for i := 1; i <= x.Deg()/2; i++ {
		// computes the polynomial (x^(2^p)-x) mod g
		if x.GCD(Pol(4).PowMod(1<<uint(i), x).Add(2)) != 1 {
			return false
		}
	}

	return true
}

// MulMod computes x*f mod g
func (x Pol) MulMod(f, g Pol) (r Pol) {
	for b := x; b != 0 && f != 0; f >>= 1 {
		if f&1 != 0 {
			r = r.Add(b).Mod(g)
		}
		b = (b << 1).Mod(g) // f'(x) = f(x) * x
	}
	return
}

// PowMod computes x^n mod g. This is needed for the reducibility test.
func (x Pol) PowMod(n uint, g Pol) (r Pol) {
	var b Pol
	for b, r = x, 1; n != 0; n >>= 1 {
		if n&1 != 0 {
			r = r.MulMod(b, g)
		}
		b = b.MulMod(b, g)
	}
	return
}

// MarshalJSON returns the JSON representation of the Pol.
func (x Pol) MarshalJSON() ([]byte, error) {
	buf := strconv.AppendUint([]byte{'"'}, uint64(x), 16)
	buf = append(buf, '"')
	return buf, nil
}

// UnmarshalJSON parses a Pol from the JSON data.
func (x *Pol) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return errors.New("invalid string for polynomial")
	}
	n, err := strconv.ParseUint(string(data[1:len(data)-1]), 16, 64)
	if err != nil {
		return err
	}
	*x = Pol(n)

	return nil
}
