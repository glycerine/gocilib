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
		if !bytes.Equal(n[:], elt.inp) {
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
		}
	}
}
