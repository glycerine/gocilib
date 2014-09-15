/*
Copyright 2014 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gocilib

import (
	"bytes"
	"strconv"
	"strings"

	"speter.net/go/exp/math/dec/inf"
)

// http://docs.oracle.com/cd/B10500_01/appdev.920/a96584/oci03typ.htm#421773
/*
Oracle stores values of the NUMBER datatype in a variable-length format. The
first byte is the exponent and is followed by 1 to 20 mantissa bytes. The
high-order bit of the exponent byte is the sign bit; it is set for positive
numbers and it is cleared for negative numbers. The lower 7 bits represent the
exponent, which is a base-100 digit with an offset of 65.

To calculate the decimal exponent, add 65 to the base-100 exponent and add
another 128 if the number is positive. If the number is negative, you do the
same, but subsequently the bits are inverted. For example, -5 has a base-100
exponent = 62 (0x3e). The decimal exponent is thus (~0x3e) -128 - 65 = 0xc1
-128 -65 = 193 -128 -65 = 0.

Each mantissa byte is a base-100 digit, in the range 1..100. For positive
numbers, the digit has 1 added to it. So, the mantissa digit for the value 5 is
6. For negative numbers, instead of adding 1, the digit is subtracted from 101.
So, the mantissa digit for the number -5 is 96 (101 - 5). Negative numbers have
a byte containing 102 appended to the data bytes. However, negative numbers
that have 20 mantissa bytes do not have the trailing 102 byte. Because the
mantissa digits are stored in base 100, each byte can represent 2 decimal
digits. The mantissa is normalized; leading zeroes are not stored.

Up to 20 data bytes can represent the mantissa. However, only 19 are guaranteed
to be accurate. The 19 data bytes, each representing a base-100 digit, yield a
maximum precision of 38 digits for an Oracle NUMBER.
*/

/*
orl.h

#define OCI_NUMBER_SIZE 22
struct OCINumber
{
  ub1 OCINumberPart[OCI_NUMBER_SIZE];
};
typedef struct OCINumber OCINumber;
*/
const OciNumberSize = 22

type OCINumber [OciNumberSize]byte

func (n OCINumber) Valid() bool {
	return !(n[0] == 0xff || n[0] == 0 && n[1] == 0)
}

// http://www.orafaq.com/wiki/NUMBER
//
func (n OCINumber) String() string {
	if !n.Valid() {
		return ""
	}
	var backingArr [127]byte
	txt := backingArr[:]
	// (number) = (sign) 0.(mantissa100 * 100**(exponent100)
	length := n[0] - 1
	if length == 0 {
		return "0"
	}
	if length > OciNumberSize {
		length = OciNumberSize
	}
	first := n[1]

	// The high-order bit of the exponent byte is the sign bit
	// it is set for positive numbers and it is cleared for negative numbers
	positive := first&0x80 > 0

	// The lower 7 bits represent the exponent, which is a base-100 digit with an offset of 65.
	exp := int(first & 0x7f)
	sign := ""
	var i, j byte
	if positive {
		exp = 2 * int((byte(exp) + 128 + 64))
		var j byte
		for j = 0; j < length; j++ {
			// Each mantissa byte is a base-100 digit, in the range 1..100.
			// For positive numbers, the digit has 1 added to it.
			digit := n[j+2] - 1
			txt[i], txt[i+1] = '0'+digit/10, '0'+digit%10
			i += 2
		}
	} else {
		exp = 2 * (int(^byte(exp)) - 128 - 64)
		sign = "-"

		if length < OciNumberSize-1 && n[length+1] == 102 {
			length--
		}

		for j = 2; j < length+2; j++ {
			digit := 101 - n[j]
			txt[i], txt[i+1] = '0'+digit/10, '0'+digit%10
			i += 2
		}
	}
	if txt[0] == '0' {
		txt = txt[1:]
		i--
		exp--
	}
	if exp <= 0 {
		return sign + "." + strings.Repeat("0", -exp) + string(bytes.TrimRight(txt[:i], "0"))
	}
	if exp < int(i) {
		// strip following zeroes
		for j = i - 1; int(j) >= exp; j-- {
			if txt[j] == '0' {
				i--
			} else {
				break
			}
		}
		return sign + string(txt[:exp]) + "." + string(txt[exp:i])
	}
	if exp > int(i) {
		return sign + string(txt[:i]) + strings.Repeat("0", exp-int(i))
	}
	return sign + string(txt[:i])
}

func (n *OCINumber) SetBytes(data []byte) *OCINumber {
	copy(n[:], data)
	return n
}

func (n *OCINumber) SetString(txt string) *OCINumber {
	if false {
		on := GetCOCINumber(txt)
		n.SetCOCINumberP(on)
		return n
	}

	if n == nil {
		return nil
	}
	if txt == "" {
		n[0] = 0xff
		return n
	}
	positive := true
	if txt[0] == '-' { // negative
		positive = false
		txt = txt[1:]
	}
	if txt == "0" {
		n[0], n[1] = 1, 128
		return n
	}
	txt = strings.TrimLeft(txt, "0")
	dot := strings.IndexByte(txt, '.')
	if dot >= 0 {
		txt = txt[:dot] + strings.TrimRight(txt[dot+1:], "0")
		if dot%2 == 1 {
			txt = "0" + txt
		}
		if len(txt)%2 > 0 {
			txt = txt + "0"
		}
	} else {
		dot = len(txt)
		for strings.HasSuffix(txt, "00") {
			txt = txt[:len(txt)-2]
		}
		if len(txt)%2 > 0 {
			txt = "0" + txt
		}
	}
	j := 1
	if positive {
		n[j] = byte((dot+1)/2 + 128 + 64)
		for i := 0; i < len(txt); i += 2 {
			j++
			n[j] = 1 + ((txt[i]-'0')*10 + txt[i+1] - '0')
		}
	} else {
		n[j] = ^byte((dot+1)/2 + 128 + 64)
		for i := 0; i < len(txt); i += 2 {
			j++
			n[j] = 101 - ((txt[i]-'0')*10 + txt[i+1] - '0')
		}
		if n[j] == 101 {
			n[j] = 102
		} else if j < OciNumberSize-1 {
			j++
			n[j] = 102
		}
	}
	n[0] = byte(j)
	return n
}

func (n *OCINumber) SetFloat(f float64) *OCINumber {
	return n.SetString(strconv.FormatFloat(f, 'g', 38, 64))
}

func (n *OCINumber) SetInt(i int64) *OCINumber {
	return n.SetString(strconv.FormatInt(i, 10))
}

func (n *OCINumber) SetDec(dec *inf.Dec) *OCINumber {
	return n.SetString(dec.String())
}

func (n *OCINumber) Set(m *OCINumber) *OCINumber {
	return n.SetBytes(m[:])
}

// Dec sets the given inf.Dec to the value of OCINumber.
func (n OCINumber) Dec(dec *inf.Dec) *inf.Dec {
	if dec == nil {
		dec = inf.NewDec(n.Unscaled(), inf.Scale(n.Scale()))
	}
	dec.SetString(n.String())
	return dec
}

// Unscaled returns the unscaled value.
func (n OCINumber) Unscaled() int64 {
	length := n[0]
	if length > OciNumberSize-2 {
		length = OciNumberSize - 2
	}
	first := n[1]
	positive := first&0x80 > 0
	exp := first & 0x7f
	var unscaled int64
	if positive {
		exp = exp + 128 + 64
		for j := byte(0); j < length; j++ {
			if j == length-1 && n[j+2] == 0 {
				break
			}
			digit := n[j+2] - 1
			unscaled = unscaled*100 + int64(digit)
		}
	} else {
		for j := byte(0); j < length; j++ {
			digit := 101 - n[j+2]
			unscaled = unscaled*100 + int64(digit)
		}
		unscaled = -unscaled
	}
	return unscaled
}

// Scale returns the scale.
func (n OCINumber) Scale() int32 {
	first := n[1]
	positive := first&0x80 > 0
	exp := first & 0x7f
	if positive {
		return int32(exp + 128 + 64)
	}
	return int32(^exp - 128 - 64)
}
