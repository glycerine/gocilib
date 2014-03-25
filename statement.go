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

// #cgo LDFLAGS: -locilib
// #include "ocilib.h"
//
// const int sof_voidp = sizeof(void*);
import "C"

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

var BindArraySize = C.uint(1000)

type Statement struct {
	handle *C.OCI_Statement
	verb   string
}

func (conn *Connection) NewStatement() (*Statement, error) {
	stmt := Statement{handle: C.OCI_StatementCreate(conn.handle)}
	if stmt.handle == nil {
		return nil, getLastErr()
	}
	return &stmt, nil
}

// NewPreparedStatement is a conveniance function for NewStatement.Prepare(qry).
func (conn *Connection) NewPreparedStatement(qry string) (*Statement, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	return stmt, stmt.Prepare(qry)
}

func (stmt *Statement) Close() error {
	if stmt.handle != nil {
		if C.OCI_StatementFree(stmt.handle) != C.TRUE {
			return getLastErr()
		}
		stmt.handle = nil
		stmt.verb = ""
	}
	return nil
}

func (stmt *Statement) Prepare(qry string) error {
	if C.OCI_Prepare(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.verb = ""
	return nil
}

func (stmt *Statement) Execute(qry string) error {
	if C.OCI_ExecuteStmt(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.verb = ""
	return nil
}

// BindExecute binds the given variables (array or map) and then executes the statement.
func (stmt *Statement) BindExecute(
	qry string,
	arrayArgs []driver.Value,
	mapArgs map[string]driver.Value,
) error {
	if C.OCI_Prepare(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	if C.OCI_BindArraySetSize(stmt.handle, BindArraySize) != C.TRUE {
		return getLastErr()
	}
	if len(arrayArgs) > 0 {
		for i, a := range arrayArgs {
			if err := stmt.BindPos(i+1, a); err != nil {
				return err
			}
		}
	} else if len(mapArgs) > 0 {
		for k, a := range mapArgs {
			if err := stmt.BindName(k, a); err != nil {
				return err
			}
		}
	}
	if C.OCI_Execute(stmt.handle) != C.TRUE {
		return getLastErr()
	}
	return nil
}

func (stmt *Statement) Verb() string {
	if stmt.verb == "" {
		stmt.verb = C.GoString(C.OCI_GetSQLVerb(stmt.handle))
	}
	return stmt.verb
}

func (stmt *Statement) IsDDL() bool {
	switch stmt.Verb() {
	case "SELECT", "INSERT", "UPDATE", "DELETE":
		return false
	default:
		return true
	}
}

func (stmt *Statement) RowsAffected() int64 {
	if stmt.Verb() == "SELECT" {
		return int64(C.OCI_GetRowCount(C.OCI_GetResultset(stmt.handle)))
	}
	return int64(C.OCI_GetAffectedRows(stmt.handle))
}

func (stmt *Statement) BindPos(pos int, arg driver.Value) error {
	return stmt.BindName(":"+strconv.Itoa(pos), arg)
}

func (stmt *Statement) BindName(name string, value driver.Value) error {
	h, nm, ok := stmt.handle, C.CString(name), C.int(C.FALSE)
Outer:
	switch x := value.(type) {
	case int16: // short
		ok = C.OCI_BindShort(h, nm, (*C.short)(unsafe.Pointer(&x)))
	case []int16:
		ok = C.OCI_BindArrayOfShorts(h, nm, (*C.short)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint16: // unsigned short
		ok = C.OCI_BindUnsignedShort(h, nm, (*C.ushort)(unsafe.Pointer(&x)))
	case []uint16:
		ok = C.OCI_BindArrayOfUnsignedShorts(h, nm, (*C.ushort)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case int: // int
		ok = C.OCI_BindInt(h, nm, (*C.int)(unsafe.Pointer(&x)))
	case []int:
		ok = C.OCI_BindArrayOfInts(h, nm, (*C.int)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint: // int
		ok = C.OCI_BindUnsignedInt(h, nm, (*C.uint)(unsafe.Pointer(&x)))
	case []uint:
		ok = C.OCI_BindArrayOfUnsignedInts(h, nm, (*C.uint)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case int64:
		ok = C.OCI_BindBigInt(h, nm, (*C.big_int)(unsafe.Pointer(&x)))
	case []int64:
		ok = C.OCI_BindArrayOfBigInts(h, nm, (*C.big_int)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint64:
		ok = C.OCI_BindUnsignedBigInt(h, nm, (*C.big_uint)(unsafe.Pointer(&x)))
	case []uint64:
		ok = C.OCI_BindArrayOfUnsignedBigInts(h, nm, (*C.big_uint)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case string:
		ok = C.OCI_BindString(h, nm, C.CString(x), C.uint(len(x)))
	case []string:
		m := 0
		for _, s := range x {
			if len(s) > m {
				m = len(s)
			}
		}
		if m == 0 {
			m = 32767
		}
		y := make([]byte, m*len(x))
		for i, s := range x {
			copy(y[i*m:(i+1)*m], []byte(s))
		}
		ok = C.OCI_BindArrayOfStrings(h, nm, (*C.dtext)(unsafe.Pointer(&x[0])), C.uint(m), C.uint(len(x)))
	case []byte:
		ok = C.OCI_BindRaw(h, nm, unsafe.Pointer(&x[0]), C.uint(len(x)))
	case [][]byte:
		m := 0
		for _, b := range x {
			if len(b) > m {
				m = len(b)
			}
		}
		if m == 0 {
			m = 32767
		}
		y := make([]byte, m*len(x))
		for i, b := range x {
			copy(y[i*m:(i+1)*m], b)
		}
		ok = C.OCI_BindArrayOfRaws(h, nm, unsafe.Pointer(&y[0]), C.uint(m), C.uint(len(x)))
	case float32:
		ok = C.OCI_BindFloat(h, nm, (*C.float)(&x))
	case []float32:
		ok = C.OCI_BindArrayOfFloats(h, nm, (*C.float)(&x[0]), C.uint(len(x)))
	case float64:
		ok = C.OCI_BindDouble(h, nm, (*C.double)(&x))
	case []float64:
		ok = C.OCI_BindArrayOfDoubles(h, nm, (*C.double)(&x[0]), C.uint(len(x)))
	case time.Time:
		od := C.OCI_DateCreate(C.OCI_StatementGetConnection(stmt.handle))
		y, m, d := x.Date()
		H, M, S := x.Clock()
		if C.OCI_DateSetDateTime(od, C.int(y), C.int(m), C.int(d), C.int(H), C.int(M), C.int(S)) != C.TRUE {
			break
		}
		ok = C.OCI_BindDate(h, nm, od)
	case []time.Time:
		od := C.OCI_DateArrayCreate(C.OCI_StatementGetConnection(stmt.handle), C.uint(len(x)))
		sof_voidp := C.int(C.sof_voidp)
		for i, t := range x {
			y, m, d := t.Date()
			H, M, S := t.Clock()
			if C.OCI_DateSetDateTime(
				(*C.OCI_Date)(unsafe.Pointer(
					uintptr(unsafe.Pointer(od))+uintptr(sof_voidp*C.int(i))),
				),
				C.int(y), C.int(m), C.int(d), C.int(H), C.int(M), C.int(S),
			) != C.TRUE {
				break Outer
			}
		}
	case time.Duration:
		oi := C.OCI_IntervalCreate(C.OCI_StatementGetConnection(stmt.handle), C.OCI_INTERVAL_DS)
		d, H, M, S, F := durationAsDays(x)
		if C.OCI_IntervalSetDaySecond(oi, C.int(d), C.int(H), C.int(M), C.int(S), C.int(F)) != C.TRUE {
			break
		}
		ok = C.OCI_BindInterval(h, nm, oi)
	default:
		return fmt.Errorf("BindName(%s): unknown type %T", name, value)
	}
	if ok != C.TRUE {
		return getLastErr()
	}
	return nil
}
