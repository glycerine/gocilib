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
	"io"
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
	//ret := (*C.OCINumber)(unsafe.Pointer(&n[0]))
	ret := GetCOCINumber(n.String())
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
	n    *C.OCINumber
	char string
	dump string
}

func makeRandomNumbers(dst chan<- randNum, conn *Connection) error {
	defer close(dst)
	qry := `
		SELECT n, TO_CHAR(n, 'TM9'), DUMP(n) 
		  FROM (SELECT dbms_random.value + dbms_random.value(-999999999999999999999,
					999999999999999999999) n
				  FROM (SELECT NULL FROM all_objects))`
	st, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Execute(qry); err != nil {
		return err
	}
	rs, err := st.Results()
	if err != nil {
		return err
	}
	defer rs.Close()

	var n C.OCINumber
	nC := NewStringVar("", 1000)
	dmp := NewStringVar("", 1000)
	row := []driver.Value{n, &nC, &dmp}
	for {
		if err = rs.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return err
			break
		}
		if err = rs.FetchInto(row); err != nil {
			return err
			break
		}
		dst <- randNum{&n, nC.String(), dmp.String()}
		break
	}
	return nil
}
