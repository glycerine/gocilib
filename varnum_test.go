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
	"testing"
)

func TestOCINumber(t *testing.T) {
	for i, elt := range []struct {
		inp []byte
		out string
	}{
		{[]byte{1, 128}, "0"},
		{[]byte{2, 193, 2}, "1"},
		{[]byte{2, 192, 2}, ".1"},
		{[]byte{2, 193, 13}, "12"},
		{[]byte{3, 193, 2, 21}, "1.2"},
		{[]byte{2, 192, 13}, ".12"},
		{[]byte{3, 194, 2, 24}, "123"},
		{[]byte{3, 193, 13, 31}, "12.3"},
		{[]byte{3, 193, 2, 24}, "1.23"},
		{[]byte{3, 192, 13, 31}, ".123"},
		{[]byte{3, 194, 13, 35}, "1234"},
		{[]byte{3, 195, 3, 9}, "20800"},
		{[]byte{3, 61, 61, 102}, "-4000"},
		{[]byte{3, 63, 100, 102}, "-.1"},
		{[]byte{21, 52, 92, 80, 53, 90, 70, 61, 95, 14, 32, 44, 47, 7, 17, 53, 39, 51, 19, 8, 65, 31}, "-921481131400687695754.94844862508293367"},
		{[]byte{21, 53, 46, 11, 38, 37, 86, 76, 77, 14, 71, 32, 98, 5, 4, 57, 18, 64, 51, 6, 95, 45}, "-55906364152524873069.03969744833750950656"},
		{[]byte{3, 193, 4, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "3.14"},
		{[]byte{4, 62, 98, 87, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "-3.14"},
		{[]byte{21, 203, 6, 59, 25, 41, 1, 90, 98, 37, 84, 61, 70, 15, 38, 22, 29, 18, 24, 7, 60, 2}, "558244000899736836069.143721281723065901"},
		{[]byte{2, 193, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "1"},
		{[]byte{2, 193, 3, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "2"},
		{[]byte{2, 193, 4, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "3"},
		{[]byte{2, 193, 6, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "5"},
		{[]byte{3, 0x3e, 96, 102, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "-5"},
		{[]byte{21, 192, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34}, ".3333333333333333333333333333333333333333"},
		{[]byte{21, 63, 38, 32, 59, 34, 50, 42, 78, 45, 33, 23, 3, 93, 10, 30, 4, 56, 79, 72, 72, 7}, "-.6369426751592356687898089171974522292994"},
	} {
		var n OCINumber
		n.SetBytes(elt.inp)
		got := n.String()
		t.Logf("%d. %v=%s", i, elt.inp, got)
		if got != elt.out {
			t.Errorf("%d. got %v, awaited %v.", i, got, elt.out)
			return
		}
		n.SetString(elt.out)
		if !(n[0] == elt.inp[0] && bytes.Equal(n[:n[0]], elt.inp[:n[0]])) {
			t.Errorf("%d. SetString mismatch: got %v, awaited %v.", i, n[:], elt.inp)
			return
		}
	}
}

func TestOCINumberSet(t *testing.T) {
	var n OCINumber
	for i, s := range []string{"1", "2", "3", "-5", "3.14", "-3.14"} {
		n.SetString(s)
		got := n.String()
		t.Logf("%d. %q=%q %v", i, s, got, n[:])
		if got != s {
			t.Errorf("%d. got %q, awaited %q.", i, got, s)
			return
		}
	}
}
