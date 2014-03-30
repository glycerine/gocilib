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
import "C"

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unsafe"
)

var zeroTime time.Time

func (stmt *Statement) Results() (*Resultset, error) {
	rs := C.OCI_GetResultset(stmt.handle)
	if rs == nil {
		return &Resultset{stmt: stmt}, getLastErr()
	}
	return &Resultset{handle: rs, stmt: stmt}, nil
}

type Resultset struct {
	handle *C.OCI_Resultset
	stmt   *Statement
	cols   []ColDesc
}

func (rs *Resultset) Next() error {
	if C.OCI_FetchNext(rs.handle) != C.TRUE {
		return getLastErr()
	}
	return nil
}

func (rs *Resultset) Close() error {
	rs.handle = nil
	return nil
}

func (rs *Resultset) RowsAffected() int64 {
	return rs.stmt.RowsAffected()
}

func (rs *Resultset) FetchInto(row []driver.Value) error {
	//log.Printf("%#v.FetchInto(%#v)", rs, row)
	cols := rs.Columns()
	var err error
	for i, v := range row {
		ui := C.uint(i + 1)
		//log.Printf("%d: %#v (%T)", i, v, v)
		isNull := C.OCI_IsNull(rs.handle, ui) == C.TRUE
		switch x := v.(type) {
		case int64:
			if isNull {
				row[i] = 0
			} else {
				row[i] = int64(C.OCI_GetBigInt(rs.handle, ui))
			}
		case *int64:
			if isNull {
				row[i] = nil
			} else {
				*x = int64(C.OCI_GetBigInt(rs.handle, ui))
			}
		case float64:
			if isNull {
				row[i] = 0
			} else {
				row[i] = C.OCI_GetDouble(rs.handle, ui)
			}
		case *float64:
			if isNull {
				row[i] = nil
			} else {
				*x = float64(C.OCI_GetDouble(rs.handle, ui))
			}
		case bool:
			if isNull {
				row[i] = false
				continue
			}
			row[i] = stringToBool(GString(C.OCI_GetString(rs.handle, ui)))
		case *bool:
			if isNull {
				row[i] = nil
			} else {
				*x = stringToBool(GString(C.OCI_GetString(rs.handle, ui)))
			}
		case []byte:
			if isNull {
				row[i] = x[:0]
				continue
			}
			n := C.OCI_GetRaw(rs.handle, ui, unsafe.Pointer(&x[0]), C.uint(cap(x)))
			row[i] = x[:n]
		case *[]byte:
			if isNull {
				row[i] = nil
			} else {
				n := C.OCI_GetRaw(rs.handle, ui, unsafe.Pointer(&(*x)[0]), C.uint(cap(*x)))
				*x = (*x)[:n]
			}
		case string:
			if isNull {
				row[i] = ""
				continue
			}
			row[i] = GString(C.OCI_GetString(rs.handle, ui))
		case *string:
			if isNull {
				row[i] = nil
			} else {
				*x = GString(C.OCI_GetString(rs.handle, ui))
			}
		case time.Time:
			if isNull {
				row[i] = zeroTime
				continue
			}
			row[i], err = ociDateToTime(C.OCI_GetDate(rs.handle, ui))
		case *time.Time:
			if isNull {
				row[i] = nil
			} else {
				*x, err = ociDateToTime(C.OCI_GetDate(rs.handle, ui))
			}
		default:
			if isNull {
				row[i] = nil
				continue
			}
			switch cols[i].Type {
			case ColNumeric:
				var s string
				if cols[i].Scale == 0 && cols[i].Scale == 0 { // FIXME(tgulacsi): how can be scale=prec=0 ?
					s = GString(C.OCI_GetString(rs.handle, ui))
					j := strings.Index(s, ".")
					neg := s[0] == '-'
					if j >= 0 {
						cols[i].Scale = len(s) - j
						cols[i].Precision = len(s) - 1
					} else {
						cols[i].Precision = len(s)
					}
					if neg {
						cols[i].Precision--
					}
				}
				if cols[i].Scale == 0 { // integer
					//fmt.Printf("col[%d]=%+v\n", i, cols[i])
					if cols[i].Precision <= 19 {
						row[i] = C.OCI_GetBigInt(rs.handle, ui)
					} else {
						if s == "" {
							s = GString(C.OCI_GetString(rs.handle, ui))
						}
						var ok bool
						if row[i], ok = big.NewInt(0).SetString(s, 10); false && !ok {
							row[i] = s
						}
					}
				} else {
					row[i] = float64(C.OCI_GetDouble(rs.handle, ui))
				}
			case ColDate:
				row[i], err = ociDateToTime(C.OCI_GetDate(rs.handle, ui))
			case ColTimestamp:
				row[i], err = ociTimestampToTime(C.OCI_GetTimestamp(rs.handle, ui))
			case ColInterval:
				row[i], err = ociIntervalToDuration(C.OCI_GetInterval(rs.handle, ui))
			case ColRaw:
				b := make([]byte, cols[i].InternalSize)
				n := C.OCI_GetRaw(rs.handle, ui, unsafe.Pointer(&b[0]), C.uint(cap(b)))
				row[i] = b[:n]
			case ColCursor:
				row[i] = &Statement{handle: C.OCI_GetStatement(rs.handle, ui)}
			default:
				//err = fmt.Errorf("FetchInto(%d.): unknown type %T", i, x)
				row[i] = GString(C.OCI_GetString(rs.handle, ui))
			}
		}
		if err != nil {
			break
		}
	}
	return err
}

type ColType uint8

const (
	ColNumeric    = ColType(C.OCI_CDT_NUMERIC)    // short, int, long long, float, double
	ColDate       = ColType(C.OCI_CDT_DATETIME)   // OCI_Date *
	ColText       = ColType(C.OCI_CDT_TEXT)       // dtext *
	ColLong       = ColType(C.OCI_CDT_LONG)       // OCI_Long *
	ColCursor     = ColType(C.OCI_CDT_CURSOR)     // OCI_Statement *
	ColLob        = ColType(C.OCI_CDT_LOB)        // OCI_Lob *
	ColFile       = ColType(C.OCI_CDT_FILE)       // OCI_File *
	ColTimestamp  = ColType(C.OCI_CDT_TIMESTAMP)  // OCI_Timestamp *
	ColInterval   = ColType(C.OCI_CDT_INTERVAL)   // OCI_Interval *
	ColRaw        = ColType(C.OCI_CDT_RAW)        // void *
	ColObject     = ColType(C.OCI_CDT_OBJECT)     // OCI_Object *
	ColCollection = ColType(C.OCI_CDT_COLLECTION) // OCI_Coll *
	ColRef        = ColType(C.OCI_CDT_REF)        // OCI_Ref *
)

// ColDesc is a column's description
type ColDesc struct {
	// Name is the name of the column
	Name string

	// Type is the numeric type of the column
	Type ColType

	// TypeName is the name of the type of the column
	TypeName string

	// DisplaySize is the display (char/rune) size
	DisplaySize int

	// InternalSize is the byte size
	InternalSize int

	// Precision is the number of all digits this number-like column can hold
	Precision int

	// Scale is the number of digits after the point
	Scale int

	// Nullable is true if the column can be null
	Nullable bool
}

func (rs *Resultset) Columns() []ColDesc {
	if rs.cols == nil {
		colCount := int(C.OCI_GetColumnCount(rs.handle))
		rs.cols = make([]ColDesc, colCount)
		for i := range rs.cols {
			c := C.OCI_GetColumn(rs.handle, C.uint(i+1))
			rs.cols[i].Name = GString(C.OCI_ColumnGetName(c))
			rs.cols[i].Type = ColType(C.OCI_ColumnGetType(c))
			rs.cols[i].TypeName = GString(C.OCI_ColumnGetSQLType(c))
			rs.cols[i].InternalSize = int(C.OCI_ColumnGetSize(c))
			rs.cols[i].Precision = int(C.OCI_ColumnGetPrecision(c))
			rs.cols[i].Scale = int(C.OCI_ColumnGetScale(c))
			rs.cols[i].Nullable = C.OCI_ColumnGetNullable(c) == C.TRUE
		}
	}
	//log.Printf("rs.cols[%d]=%#v", len(rs.cols), rs.cols)
	return rs.cols
}

func stringToBool(s string) bool {
	if len(s) == 0 {
		return false
	}
	switch s[0] {
	case 'I', 'i', 't', 'T', 'Y', 'y':
		return true
	case 'n', 'N', 'f', 'F', '0':
		return false
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return true
	}
	return false
}

func ociDateToTime(od *C.OCI_Date) (time.Time, error) {
	var t time.Time
	var y, m, d, H, M, S C.int
	if C.OCI_DateGetDateTime(od, &y, &m, &d, &H, &M, &S) != C.TRUE {
		return t, getLastErr()
	}
	t = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), 0, time.Local)
	return t, nil
}

func ociTimestampToTime(od *C.OCI_Timestamp) (time.Time, error) {
	var t time.Time
	var y, m, d, H, M, S, F, oh, om C.int
	zone := time.Local
	if C.OCI_TimestampGetTimeZoneOffset(od, &oh, &om) == C.TRUE {
		var name string
		b := make([]byte, 32)
		if C.OCI_TimestampGetTimeZoneName(od, C.int(cap(b)), (*C.otext)(unsafe.Pointer(&b[0]))) == C.TRUE {
			i := bytes.IndexByte(b, 0)
			if i >= 0 {
				b = b[:i]
			}
			name = string(b)
		}
		if name == "" {
			name = fmt.Sprintf("%d:%d", oh, om)
		}
		zone = time.FixedZone(name, int(oh*60+om)*60)
	}
	if C.OCI_TimestampGetDateTime(od, &y, &m, &d, &H, &M, &S, &F) != C.TRUE {
		return t, getLastErr()
	}
	t = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), int(time.Duration(F)*time.Millisecond*100), zone)
	return t, nil
}

func ociIntervalToDuration(od *C.OCI_Interval) (time.Duration, error) {
	if C.OCI_IntervalGetType(od) == C.OCI_INTERVAL_YM {
		var y, m C.int
		if C.OCI_IntervalGetYearMonth(od, &y, &m) != C.TRUE {
			return 0, getLastErr()
		}
		return time.Duration(y*365+m*30) * 24 * time.Hour, nil
	}
	var d, H, M, S, F C.int
	if C.OCI_IntervalGetDaySecond(od, &d, &H, &M, &S, &F) != C.TRUE {
		return 0, getLastErr()
	}
	return (time.Duration(d*24+H)*time.Hour +
		time.Duration(M)*time.Minute +
		time.Duration(S)*time.Second +
		time.Duration(F*100)*time.Millisecond), nil
}
