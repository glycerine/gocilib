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
	"fmt"
	"log"
	"strconv"
	"strings"

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
func filterErr(err *error) error {
	//log.Printf("filterErr(%v)", err)
	if oraErr, ok := (*err).(*gocilib.Error); ok {
		switch oraErr.Code {
		case 115, 451, 452, 609, 1090, 1092, 1073, 3113, 3114, 3135, 3136, 12153, 12161, 12170, 12224, 12230, 12233, 12510, 12511, 12514, 12518, 12526, 12527, 12528, 12539: //connection errors - try again!
			*err = driver.ErrBadConn
		case 0:
			if oraErr.Text == "" {
				*err = nil
			}
		}
	}
	return *err
}

// Prepare the query for execution, return a prepared statement and error
func (c conn) Prepare(query string) (driver.Stmt, error) {
	st, err := c.cx.NewStatement()
	if filterErr(&err) != nil {
		return nil, fmt.Errorf("Prepare[creating statement]: %v", err)
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
		//log.Printf("%#v.Prepare(%q)", st, query)
	}
	debug("%p.Prepare(%s)", st, query)
	err = st.Prepare(query)
	if filterErr(&err) != nil {
		return nil, fmt.Errorf("Prepare query: %v", err)
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
	bindCount, err := s.st.BindCount()
	if err != nil {
		log.Printf("ERROR getting bind count: %v", err)
		return 0
	}
	return bindCount
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
	//log.Printf("%#v.BindExecute(%#v, %#v)", s.st, s.statement, args)
	if err = s.st.BindExecute(s.statement, args, nil); filterErr(&err) != nil {
		return nil, fmt.Errorf("BindExec%#v %q: %v", args, s.statement, err)
	}

	rs, err := s.st.Results()
	//log.Printf("%#v.Results(): %#v, %v", s.st, rs, err)
	if filterErr(&err) != nil {
		return nil, fmt.Errorf("BindExec %q results: %v", s.statement, err)
	}
	rr := &rowsRes{rs: rs, cols: rs.Columns()}
	//log.Printf("%#v.run(%#v): %#v", s, args, rr)
	return rr, nil
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
	//log.Printf("%#v.RowsAffected(): %d", r.rs, r.rs.RowsAffected())
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
	if r.rs != nil {
		debug("CLOSEing result %p", r.rs)
		err = r.rs.Close()
		r.rs = nil
	}
	return err
}

// DATE, DATETIME, TIMESTAMP are treated as they are in Local time zone
func (r rowsRes) Next(dest []driver.Value) error {
	if err := r.rs.Next(); err != nil {
		return err
	}
	err := r.rs.FetchInto(dest)
	//log.Printf("%#v.FetchInto(%#v): %v", r.rs, dest, err)
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
