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
