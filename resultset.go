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

func (stmt *Statement) ResultSet() (*Resultset, error) {
	rs := C.OCI_GetResultset(stmt.handle)
	if rs == nil {
		return &Resultset{stmt: stmt}, getLastErr()
	}
	return &Resultset{handle: rs, stmt: stmt}, nil
}

type Resultset struct {
	handle *C.OCI_Resultset
	stmt   *Statement
}

func (rs *Resultset) Next() error {
	if C.OCI_FetchNext(rs.handle) != C.TRUE {
		return getLastErr()
	}
	return nil
}

func (rs *Resultset) RowsAffected() int64 {
	return rs.stmt.RowsAffected()
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
	colCount := C.OCI_GetColumnCount(rs.handle)
	cols := make([]ColDesc, int(colCount))
	for i := C.uint(1); i <= colCount; i++ {
		c := C.OCI_GetColumn(rs.handle, i)
		cols[i].Name = C.GoString(C.OCI_ColumnGetName(c))
		cols[i].Type = ColType(C.OCI_ColumnGetType(c))
		cols[i].TypeName = C.GoString(C.OCI_ColumnGetSQLType(c))
		cols[i].InternalSize = int(C.OCI_ColumnGetSize(c))
		cols[i].Precision = int(C.OCI_ColumnGetPrecision(c))
		cols[i].Scale = int(C.OCI_ColumnGetScale(c))
		cols[i].Nullable = C.OCI_ColumnGetNullable(c) == C.TRUE
	}
	return cols
}
