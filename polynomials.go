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
func (x Pol) Mul(y Pol) Pol {
	switch {
	case x == 0 || y == 0:
		return 0
	case x == 1:
		return y
	case y == 1:
		return x
	case y == 2:
		return x.mul2()
	}

	var res Pol
	for i := 0; i <= y.Deg(); i++ {
		if (y & (1 << uint(i))) > 0 {
			res = res.Add(x << uint(i))
		}
	}

	if res.Div(y) != x {
		panic("multiplication would overflow uint64")
	}

	return res
}

// 2*x.
func (x Pol) mul2() Pol {
	if x&(1<<63) != 0 {
		panic("multiplication would overflow uint64")
	}
	return x << 1
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
func (x Pol) DivMod(d Pol) (Pol, Pol) {
	if x == 0 {
		return 0, 0
	}

	if d == 0 {
		panic("division by zero")
	}

	D := d.Deg()
	diff := x.Deg() - D
	if diff < 0 {
		return 0, x
	}

	var q Pol
	for diff >= 0 {
		m := d << uint(diff)
		q |= (1 << uint(diff))
		x = x.Add(m)

		diff = x.Deg() - D
	}

	return q, x
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

	if x == 0 {
		return f
	}

	if x.Deg() < f.Deg() {
		x, f = f, x
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
		if x.GCD(qp(uint(i), x)) != 1 {
			return false
		}
	}

	return true
}

// MulMod computes x*f mod g
func (x Pol) MulMod(f, g Pol) Pol {
	if x == 0 || f == 0 {
		return 0
	}

	var res Pol
	for i := 0; i <= f.Deg(); i++ {
		if (f & (1 << uint(i))) > 0 {
			a := x
			for j := 0; j < i; j++ {
				a = a.Mul(2).Mod(g)
			}
			res = res.Add(a).Mod(g)
		}
	}

	return res
}

// qp computes the polynomial (x^(2^p)-x) mod g. This is needed for the
// reducibility test.
func qp(p uint, g Pol) Pol {
	num := (1 << p)
	i := 1

	// start with x
	res := Pol(2)

	for i < num {
		// repeatedly square res
		res = res.MulMod(res, g)
		i *= 2
	}

	// add x
	return res.Add(2).Mod(g)
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
