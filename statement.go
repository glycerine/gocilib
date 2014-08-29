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
	"errors"
)

const defaultPrefetchMemory = 1 << 20 // 1Mb
const defaultFetchSize = 100

// BindArraySize is the size of bind arrays
var BindArraySize = C.uint(1000)

// ErrEmptyStatement
var ErrEmptyStatement = errors.New("empty statement")

// Statement holds the OCI_Statement handle.
//
// PrefetchMemory and FetchSize are set in Statement.Prepare
type Statement struct {
	handle                    *C.OCI_Statement
	statement, verb           string
	bindCount                 int
	PrefetchMemory, FetchSize uint
}

// NewStatement creates a new statement
func (conn *Connection) NewStatement() (*Statement, error) {
	stmt := Statement{handle: C.OCI_StatementCreate(conn.handle),
		PrefetchMemory: defaultPrefetchMemory, FetchSize: defaultFetchSize,
	}
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

// Close closes the statement.
func (stmt *Statement) Close() error {
	if stmt.handle != nil {
		if C.OCI_StatementFree(stmt.handle) != C.TRUE {
			return getLastErr()
		}
		stmt.handle = nil
		stmt.statement, stmt.verb, stmt.bindCount = "", "", 0
	}
	return nil
}

// Prepare the query for execution.
// After Prepare, you can Execute/BindExecute the statement already prepared,
// by executing with empty qry.
func (stmt *Statement) Prepare(qry string) error {
	if C.OCI_Prepare(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.statement, stmt.verb = qry, ""
	stmt.bindCount = int(C.OCI_GetBindCount(stmt.handle))
	return stmt.setFetchSizes()
}

// Execute the given query.
// If qry is "", then the previously prepared/executed query string is used.
func (stmt *Statement) Execute(qry string) error {
	if qry == "" {
		qry = stmt.statement
	}
	if qry == "" {
		return ErrEmptyStatement
	}
	if C.OCI_ExecuteStmt(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	stmt.statement = qry
	if err := stmt.setFetchSizes(); err != nil {
		return err
	}
	stmt.verb = C.GoString(C.OCI_GetSQLVerb(stmt.handle))
	stmt.bindCount = int(C.OCI_GetBindCount(stmt.handle))
	return nil
}

// BindExecute binds the given variables (array or map) and then executes the statement.
// This will fetch the bound variables after the execute, for each OUT binds.
func (stmt *Statement) BindExecute(
	qry string,
	arrayArgs []driver.Value,
	mapArgs map[string]driver.Value,
) error {
	if qry == "" {
		qry = stmt.statement
	}
	if qry == "" {
		return ErrEmptyStatement
	}
	if qry != stmt.statement {
		if C.OCI_Prepare(stmt.handle, C.CString(qry)) != C.TRUE {
			return getLastErr()
		}
		if err := stmt.setFetchSizes(); err != nil {
			return err
		}
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

	if len(arrayArgs) > 0 {
		for i, value := range arrayArgs {
			bnd := C.OCI_GetBind(stmt.handle, C.uint(i+1))
			if C.OCI_BindGetDirection(bnd) == C.OCI_BDM_IN {
				continue
			}
			var err error
			if arrayArgs[i], err = getBindInto(value, bnd); err != nil {
				return err
			}
		}
	} else if len(mapArgs) > 0 {
		for nm, value := range mapArgs {
			bnd := C.OCI_GetBind2(stmt.handle, C.CString(nm))
			if C.OCI_BindGetDirection(bnd) == C.OCI_BDM_IN {
				continue
			}
			var err error
			if mapArgs[nm], err = getBindInto(value, bnd); err != nil {
				return err
			}
		}
	}

	return nil
}

func (stmt *Statement) setFetchSizes() error {
	if stmt.verb != "SELECT" {
		return nil
	}
	if C.OCI_SetPrefetchMemory(stmt.handle, C.uint(stmt.PrefetchMemory)) != C.TRUE {
		return getLastErr()
	}
	if C.OCI_SetFetchSize(stmt.handle, C.uint(stmt.FetchSize)) != C.TRUE {
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

// Parse will send the qry for parsing to the server.
// Only good for testing parse errors.
func (stmt *Statement) Parse(qry string) error {
	if C.OCI_Parse(stmt.handle, C.CString(qry)) != C.TRUE {
		return getLastErr()
	}
	return nil
}

// QueryRow mimics *sql.DB.QueryRow, in that it executes the query and then
// fetches the first row into dest.
func (stmt *Statement) QueryRow(qry string, args []driver.Value, dest []driver.Value) error {
	var err error
	Log.Debug("QueryRow", "qry", qry, "args", args)
	if len(args) > 0 {
		err = stmt.BindExecute(qry, args, nil)
	} else {
		err = stmt.Execute(qry)
	}
	if err != nil {
		return err
	}
	rs, err := stmt.Results()
	if err != nil {
		return err
	}
	defer rs.Close()
	if err = rs.Next(); err != nil {
		return err
	}
	Log.Debug("FI", "dest", dest)
	err = rs.FetchInto(dest)
	Log.Debug("FI", "dest", dest, "error", err)
	return err
}
