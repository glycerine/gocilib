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
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

//UseBigInt shall be true for returning big.Ints
var UseBigInt = false

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
		err := getLastErr()
		if err != nil && err.(*Error).Code != 0 {
			return err
		}
		return io.EOF
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
		Log.Debug("FetchInto", "i", i, "v", fmt.Sprintf("%#v (%T)", v, v))
		isNull := C.OCI_IsNull(rs.handle, ui) == C.TRUE
		switch x := v.(type) {
		case int:
			if isNull {
				row[i] = 0
			} else {
				row[i] = int(C.OCI_GetBigInt(rs.handle, ui))
			}
		case *int:
			if isNull {
				row[i] = nil
			} else {
				*x = int(C.OCI_GetBigInt(rs.handle, ui))
			}
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
			row[i] = stringToBool(C.GoString(C.OCI_GetString(rs.handle, ui)))
		case *bool:
			if isNull {
				row[i] = nil
			} else {
				*x = stringToBool(C.GoString(C.OCI_GetString(rs.handle, ui)))
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
			row[i] = C.GoString(C.OCI_GetString(rs.handle, ui))
		case *string:
			if isNull {
				row[i] = nil
			} else {
				*x = C.GoString(C.OCI_GetString(rs.handle, ui))
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
			ref := reflect.ValueOf(row[i])
			isPointer := ref.Kind() == reflect.Ptr
			pointerOk := isPointer
			if isPointer && !ref.IsNil() {
				ref = ref.Elem()
				pointerOk = ref.CanSet()
			}
			Log.Debug("default", "Type", ref.Type(), "isPointer?", isPointer, "pointerOk?", pointerOk, "kind", ref.Kind(),
				"col.Type", cols[i].Type)
			switch cols[i].Type {
			case ColNumeric:
				s := strings.Replace(C.GoString(C.OCI_GetString(rs.handle, ui)), ",", ".", 1)
				if cols[i].Precision <= 0 && cols[i].Scale <= 0 { // FIXME(tgulacsi): how can be scale=prec=0 ?
					cols[i].Precision, cols[i].Scale = len(s), 0
					if s[0] == '-' {
						cols[i].Precision--
					}
					j := strings.IndexByte(s, '.')
					if j >= 0 {
						cols[i].Precision--
						cols[i].Scale = len(s) - 1
					}
				}
				Log.Debug("int", "prec", cols[i].Precision, "scale", cols[i].Scale)
				if cols[i].Scale == 0 { // integer
					//fmt.Printf("col[%d]=%+v\n", i, cols[i])
					if cols[i].Precision <= 19 {
						x := int64(C.OCI_GetBigInt(rs.handle, ui))
						Log.Debug("ref", "ref", ref, "x", x)
						if isPointer {
							if !pointerOk {
								row[i] = &x
							} else {
								Log.Debug("set", "x", x, "vof", reflect.ValueOf(x))
								ref.SetInt(x)
								Log.Debug("set", "ref", ref)
							}
						} else {
							row[i] = x
						}
					} else {
						ok := false
						if UseBigInt {
							row[i], ok = row[i].(*big.Int).SetString(s, 10)
						}
						if !ok {
							if isPointer {
								if pointerOk {
									ref.Set(reflect.ValueOf(s))
								} else {
									row[i] = &s
								}
							} else {
								row[i] = s
							}
						}
					}
				} else {
					ok := false
					if UseBigInt {
						row[i], ok = row[i].(*big.Rat).SetString(s)
					}
					if !ok {
						if isPointer {
							if pointerOk {
								ref.Set(reflect.ValueOf(x))
							} else {
								row[i] = &s
							}
						} else {
							row[i] = s
						}
					}
					//row[i] = float64(C.OCI_GetDouble(rs.handle, ui))
				}
			case ColDate:
				var t time.Time
				t, err = ociDateToTime(C.OCI_GetDate(rs.handle, ui))
				if isPointer {
					if pointerOk {
						ref.Set(reflect.ValueOf(t))
					} else {
						row[i] = &t
					}
				} else {
					row[i] = t
				}
			case ColTimestamp:
				var t time.Time
				t, err = ociTimestampToTime(C.OCI_GetTimestamp(rs.handle, ui))
				if isPointer {
					if pointerOk {
						ref.Set(reflect.ValueOf(t))
					} else {
						row[i] = &t
					}
				} else {
					row[i] = t
				}
			case ColInterval:
				var t time.Duration
				t, err = ociIntervalToDuration(C.OCI_GetInterval(rs.handle, ui))
				if isPointer {
					ref.Set(reflect.ValueOf(t))
				} else {
					row[i] = t
				}
			case ColRaw:
				b := make([]byte, cols[i].InternalSize)
				n := C.OCI_GetRaw(rs.handle, ui, unsafe.Pointer(&b[0]), C.uint(cap(b)))
				row[i] = b[:n]
			case ColCursor:
				st := Statement{handle: C.OCI_GetStatement(rs.handle, ui)}
				if isPointer && pointerOk {
					ref.Set(reflect.ValueOf(&st))
				} else {
					row[i] = &st
				}
			default:
				//err = fmt.Errorf("FetchInto(%d.): unknown type %T", i, x)
				row[i] = C.GoString(C.OCI_GetString(rs.handle, ui))
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

func (ct ColType) String() string {
	switch ct {
	case ColNumeric:
		return "NUMBER"
	case ColDate:
		return "DATE"
	case ColText:
		return "STRING"
	case ColLong:
		return "LONG"
	case ColCursor:
		return "CURSOR"
	case ColLob:
		return "LOB"
	case ColFile:
		return "FILE"
	case ColTimestamp:
		return "TIMESTAMP"
	case ColInterval:
		return "INTERVAL"
	case ColRaw:
		return "RAW"
	case ColObject:
		return "OBJECT"
	case ColCollection:
		return "COLLECTION"
	case ColRef:
		return "REF"
	default:
		return "???"
	}
}

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
			rs.cols[i].Name = C.GoString(C.OCI_ColumnGetName(c))
			rs.cols[i].Type = ColType(C.OCI_ColumnGetType(c))
			rs.cols[i].TypeName = C.GoString(C.OCI_ColumnGetSQLType(c))
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
		if C.OCI_TimestampGetTimeZoneName(od, C.int(cap(b)), (*C.char)(unsafe.Pointer(&b[0]))) == C.TRUE {
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
