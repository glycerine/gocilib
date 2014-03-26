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
import "C"

import (
	"database/sql/driver"
)

var BindArraySize = C.uint(1000)

type Statement struct {
	handle    *C.OCI_Statement
	verb      string
	bindCount int
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
		stmt.verb, stmt.bindCount = "", 0
	}
	return nil
}

func (stmt *Statement) Prepare(qry string) error {
	if C.OCI_Prepare(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.verb = C.GoString(C.OCI_GetSQLVerb(stmt.handle))
	stmt.bindCount = int(C.OCI_GetBindCount(stmt.handle))
	return nil
}

func (stmt *Statement) Execute(qry string) error {
	if C.OCI_ExecuteStmt(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.verb = C.GoString(C.OCI_GetSQLVerb(stmt.handle))
	stmt.bindCount = int(C.OCI_GetBindCount(stmt.handle))
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
	//if C.OCI_BindArraySetSize(stmt.handle, BindArraySize) != C.TRUE {
	//	return getLastErr()
	//}
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

func (stmt *Statement) BindCount() (int, error) {
	if stmt.bindCount <= 0 && stmt.Verb() == "" { // haven't been Prepared/Executed yet
		names, err := getBindInfo(
			C.OCI_HandleGetStatement(stmt.handle),
			C.OCI_HandleGetError(C.OCI_StatementGetConnection(stmt.handle)),
			nil)
		if err != nil {
			return -1, err
		}
		stmt.bindCount = len(names)
	}
	return stmt.bindCount, nil
}

func (stmt *Statement) RowsAffected() int64 {
	if stmt.Verb() == "SELECT" {
		return int64(C.OCI_GetRowCount(C.OCI_GetResultset(stmt.handle)))
	}
	return int64(C.OCI_GetAffectedRows(stmt.handle))
}
