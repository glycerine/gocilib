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
	"testing"
)

func TestOCINumber(t *testing.T) {
	var n OCINumber
	for i, elt := range []struct {
		inp []byte
		out string
	}{
		{[]byte{2, 193, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "1"},
		{[]byte{2, 193, 3, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "2"},
		{[]byte{2, 193, 4, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "3"},
	} {
		n.SetBytes(elt.inp)
		got := n.String()
		t.Logf("%d. %q=%q", i, elt.inp, got)
		if got != elt.out {
			t.Errorf("%d. got %q, awaited %q.", i, got, elt.out)
		}
	}
}

func TestOCINumberSet(t *testing.T) {
	var n OCINumber
	for i, s := range []string{"1", "2", "3"} {
		n.SetString(s)
		got := n.String()
		t.Logf("%d. %q=%q %#v", i, s, got, n)
		if got != s {
			t.Errorf("%d. got %q, awaited %q.", i, got, s)
		}
	}
}
