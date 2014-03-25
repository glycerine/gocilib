/*
Package driver implements a Go Oracle driver using gocilib

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

package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"strconv"
	"strings"
	"unsafe"

	"github.com/tgulacsi/gocilib"
)

var (
	// NotImplemented prints Not implemented
	NotImplemented = errors.New("Not implemented")
	// IsDebug should we print debug logs?
	IsDebug bool
)

type conn struct {
	cx *gocilib.Connection
}

type stmt struct {
	st        *gocilib.Statement
	statement string
}

// filterErr filters the error, returns driver.ErrBadConn if appropriate
func filterErr(err error) error {
	if oraErr, ok := err.(*gocilib.Error); ok {
		switch oraErr.Code {
		case 115, 451, 452, 609, 1090, 1092, 1073, 3113, 3114, 3135, 3136, 12153, 12161, 12170, 12224, 12230, 12233, 12510, 12511, 12514, 12518, 12526, 12527, 12528, 12539: //connection errors - try again!
			return driver.ErrBadConn
		}
	}
	return err
}

// Prepare the query for execution, return a prepared statement and error
func (c conn) Prepare(query string) (driver.Stmt, error) {
	st, err := c.cx.NewStatement()
	if err != nil {
		return nil, err
	}
	if strings.Index(query, ":1") < 0 && strings.Index(query, "?") >= 0 {
		q := strings.Split(query, "?")
		q2 := make([]string, 0, 2*len(q)-1)
		for i := 0; i < len(q); i++ {
			if i > 0 {
				q2 = append(q2, ":"+strconv.Itoa(i))
			}
			q2 = append(q2, q[i])
		}
		query = strings.Join(q2, "")
	}
	debug("%p.Prepare(%s)", st, query)
	err = st.Prepare(query)
	if err != nil {
		return nil, filterErr(err)
	}
	return stmt{st: st, statement: query}, nil
}

// closes the connection
func (c conn) Close() error {
	err := c.cx.Close()
	c.cx = nil
	return err
}

type tx struct {
	cx *gocilib.Connection
}

// begins a transaction
func (c conn) Begin() (driver.Tx, error) {
	if !c.cx.IsConnected() {
		return nil, errors.New("not connected")
	}
	return tx{cx: c.cx}, nil
}

// commits currently opened transaction
func (t tx) Commit() error {
	if t.cx != nil {
		return t.cx.Commit()
	}
	return nil
}

// rolls back current transaction
func (t tx) Rollback() error {
	if t.cx != nil {
		return t.cx.Rollback()
	}
	return nil
}

// closes statement
func (s stmt) Close() error {
	if s.st != nil {
		debug("CLOSEing statement %p (%s)", s.st, s.statement)
		s.st.Close()
		s.st = nil
	}
	return nil
}

// number of input parameters
func (s stmt) NumInput() int {
	names, err := s.st.GetBindNames()
	if err != nil {
		log.Printf("error getting bind names of %p: %s", s.st, err)
		return -1
	}
	return len(names)
}

type rowsRes struct {
	rs   *gocilib.Resultset
	cols []gocilib.ColDesc
}

// executes the statement
func (s stmt) run(args []driver.Value) (*rowsRes, error) {
	//A driver Value is a value that drivers must be able to handle.
	//A Value is either nil or an instance of one of these types:
	//int64
	//float64
	//bool
	//[]byte
	//string   [*] everywhere except from Rows.Next.
	//time.Time

	var err error
	a := (*[]interface{})(unsafe.Pointer(&args))
	debug("%p.run(%s, %v)", s.st, s.statement, *a)
	if err = s.st.BindExecute(s.statement, args, nil); err != nil {
		return nil, filterErr(err)
	}

	rs, err := s.st.Resultset()
	if err != nil {
		return nil, err
	}
	return &rowsRes{rs: rs, cols: rs.Columns()}, nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.run(args)
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.run(args)
}

func (r rowsRes) LastInsertId() (int64, error) {
	return -1, NotImplemented
}

func (r rowsRes) RowsAffected() (int64, error) {
	return r.rs.RowsAffected(), nil
}

// resultset column names
func (r rowsRes) Columns() []string {
	cls := make([]string, len(r.cols))
	for i, c := range r.cols {
		cls[i] = c.Name
	}
	return cls
}

// closes the resultset
func (r rowsRes) Close() error {
	var err error
	if r.st != nil {
		debug("CLOSEing result %p", r.st)
		err = r.st.Close()
		r.st = nil
	}
	return err
}

// DATE, DATETIME, TIMESTAMP are treated as they are in Local time zone
func (r rowsRes) Next(dest []driver.Value) error {
	row := (*[]interface{})(unsafe.Pointer(&dest))
	// log.Printf("FetcOneInto(%p %+v len=%d) %T", row, *row, len(*row), *row)
	err := r.st.FetchOneInto(*row...)
	debug("fetched row=%p %+v (len=%d) err=%s", row, *row, len(*row), err)
	return err
}

// Driver implements a Driver
type Driver struct {
	// Defaults
	user, passwd, db string

	initCmds   []string
	autocommit bool
}

// Open new connection. The uri need to have the following syntax:
//
//   USER/PASSWD@SID
//
// SID (database identifier) can be a DSN (see goracle/oracle.MakeDSN)
func (d *Driver) Open(uri string) (driver.Conn, error) {
	d.user, d.passwd, d.db = gocilib.SplitDSN(uri)

	// Establish the connection
	cx, err := gocilib.NewConnection(d.user, d.passwd, d.db)
	if err != nil {
		return nil, err
	}
	if d.autocommit {
		err = cx.SetAutoCommit(true)
	}
	return &conn{cx: cx}, err
}

// use log.Printf for log messages if IsDebug
func debug(fmt string, args ...interface{}) {
	if IsDebug {
		log.Printf(fmt, args...)
	}
}

// Driver automatically registered in database/sql
var d = Driver{}

// SetAutoCommit sets auto commit mode for future connections
// true is open autocommit, default false
func SetAutoCommit(b bool) {
	d.autocommit = b
}

func init() {
	sql.Register("gocilib", &d)
}
