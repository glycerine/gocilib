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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/inconshreveable/log15.v2"
)

func TestOCINumberDB(t *testing.T) {
	if *fDsn == "" {
		t.Skip("no -dsn specified, skipping TestOCINumberDB.")
		return
	}
	if testing.Verbose() {
		Log.SetHandler(log15.StderrHandler)
	}
	conn := getConnection(t)

	dur := 10 * time.Second
	if testing.Short() {
		dur = 1 * time.Second
	}
	var (
		err        error
		st         *Statement
		rn         randNum
		stHasClose bool
	)

	start := time.Now()
	var n OCINumber
	for {
		if time.Since(start) > dur {
			break
		}
		i := 1 + rand.Intn(21)
		j := rand.Intn(i)
		pattern := strings.Repeat("9", i)
		if j > 0 {
			pattern = pattern[j:] + "." + pattern[j:]
		}
		rn, err = makeRandomNumber(conn, pattern)
		if err != nil {
			t.Fatal(err)
		}
		if stHasClose && st != nil {
			defer st.Close()
			stHasClose = true
		}
		t.Logf("%s=%s", rn.char, rn.dump)
		txt := rn.dump[10:]
		i = strings.IndexByte(txt, ':')
		length, err := strconv.Atoi(txt[:i])
		if err != nil {
			t.Fatalf("error parsing %s: %v", txt, err)
		}
		n[0] = byte(length)
		for j, txt := range strings.SplitN(txt[i+2:], ",", length) {
			k, err := strconv.Atoi(txt)
			if err != nil {
				t.Fatalf("error parsing %s: %v", txt, err)
			}
			n[j+1] = byte(k)
		}
		for j := length + 1; j < OciNumberSize; j++ {
			n[j] = 0
		}
		//t.Logf("%s => %v", rn.dump, n[:])

		got := n.String()
		if got != rn.char {
			t.Errorf("got %s, awaited %s", got, rn.char)
			break
		}
		var m OCINumber
		m.SetString(rn.char)
		if !bytes.Equal(m[:], n[:]) {
			t.Errorf("got %v, awaited %v", m[:], n[:])
			break
		}
	}
}
