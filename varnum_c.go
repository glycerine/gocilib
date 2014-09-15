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

/*
#cgo LDFLAGS: -lclntsh

#include <oci.h>
#include <stdio.h>
#include <string.h>
*/
import "C"

import (
	"database/sql/driver"
	"strings"
	"unsafe"
)

// SetCOCINumber sets this OCINumber to the same value as the given C.OCINumber.
func (n *OCINumber) SetCOCINumber(m C.OCINumber) *OCINumber {
	C.memcpy(unsafe.Pointer(&n[0]), unsafe.Pointer(&m), OciNumberSize)
	return n
}

func (n *OCINumber) SetCOCINumberP(m *C.OCINumber) *OCINumber {
	C.memcpy(unsafe.Pointer(&n[0]), unsafe.Pointer(m), OciNumberSize)
	return n
}

// COCINumber returns a *C.OCINumber, backed by this OCINumber.
func (n OCINumber) COCINumber() *C.OCINumber {
	ret := (*C.OCINumber)(unsafe.Pointer(&n[0]))
	//ret := GetCOCINumber(n.String())
	Log.Debug("COCINumber", "n", n[:], "txt", n.String(), "ret", ret)
	return ret
}

func GetCOCINumber(text string) *C.OCINumber {
	on := new(C.OCINumber)
	i := strings.IndexByte(text, '.')
	var format string
	if i < 0 {
		format = strings.Repeat("9", len(text))
	} else {
		format = strings.Repeat("9", i) + "." + strings.Repeat("9", len(text)-1-i)
	}
	Log.Debug("GetCOCINumber", "text", text, "format", format)
	C.OCINumberFromText(anErrHandle,
		(*C.oratext)(unsafe.Pointer(C.CString(text))), C.ub4(len(text)),
		(*C.oratext)(unsafe.Pointer(C.CString(format))), C.ub4(len(format)), nil, 0, on)
	return on
}

type randNum struct {
	char string
	dump string
}

func makeRandomNumber(conn *Connection, pattern string) (randNum, error) {
	var rn randNum
	qry := ""
	i := strings.IndexByte(pattern, '.')
	if i >= 0 {
		qry = "dbms_random.value + dbms_random.value(-" + pattern[:i] + ", " + pattern[:i] + ")"
	} else {
		qry = "dbms_random.value(-" + pattern + ", " + pattern + ")"
	}
	qry = `SELECT TO_CHAR(n), DUMP(n) FROM (SELECT TO_NUMBER(TO_CHAR(` + qry + `, '` + pattern + `')) n FROM DUAL)`
	st, err := conn.NewStatement()
	if err != nil {
		return rn, err
	}
	defer st.Close()
	if err := st.Execute(qry); err != nil {
		return rn, err
	}
	rs, err := st.Results()
	if err != nil {
		return rn, err
	}
	defer rs.Close()

	row := []driver.Value{&rn.char, &rn.dump}
	if err = rs.Next(); err != nil {
		return rn, err
	}
	if err = rs.FetchInto(row); err != nil {
		return rn, err
	}
	return rn, nil
}
