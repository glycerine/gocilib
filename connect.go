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
	"fmt"
	"os"
	"strings"
	"sync"
)

//SplitDSN splits username/password@sid
func SplitDSN(dsn string) (username, password, sid string) {
	if i := strings.LastIndex(dsn, "@"); i >= 0 {
		//fmt.Printf("dsn=%q (%d) i=%d\n", dsn, len(dsn), i)
		if i > 0 {
			username = dsn[:i]
		}
		if i < len(dsn)-1 {
			sid = dsn[i+1:]
		}
	} else {
		username = dsn
	}
	if i := strings.Index(username, "/"); i >= 0 {
		//fmt.Printf("username=%q (%d) i=%d\n", username, len(username), i)
		if i > 0 {
			if i < len(username) {
				password = username[i+1:]
			}
			username = username[:i]
		} else {
			username, password = "", username[1:]
		}
	}
	return
}

type Connection struct {
	handle *C.OCI_Connection
}

var connNumMu sync.Mutex
var connNum int

func NewConnection(user, passwd, sid string) (*Connection, error) {
	initialize()
	connNumMu.Lock()
	conn := Connection{
		handle: C.OCI_ConnectionCreate(C.CString(sid), C.CString(user), C.CString(passwd),
			C.OCI_SESSION_DEFAULT),
	}
	if conn.handle == nil {
		connNumMu.Unlock()
		return nil, getLastErr()
	}
	connNum++
	connNumMu.Unlock()
	return &conn, (&conn).SetAutoCommit(false)
}

func (conn *Connection) SetAutoCommit(commit bool) error {
	c := C.int(C.TRUE)
	if !commit {
		c = C.FALSE
	}
	if C.OCI_SetAutoCommit(conn.handle, c) != C.TRUE {
		return getLastErr()
	}
	return nil
}

// Close closes the connection, and cleans up if this was the last connection
func (conn *Connection) Close() error {
	var err error
	if conn.handle != nil {
		if C.OCI_ConnectionFree(conn.handle) != C.TRUE {
			err = fmt.Errorf("error closing %p", conn.handle)
		}
		conn.handle = nil
	}
	connNumMu.Lock()
	connNum--
	if connNum <= 0 {
		C.OCI_Cleanup()
	}
	connNumMu.Unlock()
	return err
}

func getLastErr() error {
	ociErr := C.OCI_GetLastError()
	return &Error{int(C.OCI_ErrorGetOCICode(ociErr)),
		C.GoString(C.OCI_ErrorGetString(ociErr))}
}

type Error struct {
	Code int
	Text string
}

func (e Error) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Text)
}

var initOnce sync.Once

func initialize() {
	initOnce.Do(func() {
		nlsLang := os.Getenv("NLS_LANG")
		if nlsLang == "" {
			os.Setenv("NLS_LANG", "american_america.AL32UTF8")
		} else {
			os.Setenv("NLS_LANG", strings.SplitN(nlsLang, ".", 2)[0]+".AL32UTF8")
		}
		if C.OCI_Initialize(nil, nil,
			C.OCI_ENV_DEFAULT|C.OCI_ENV_THREADED|C.OCI_ENV_CONTEXT|C.OCI_ENV_EVENTS,
		) != C.TRUE {
			panic("error initializing OCILIB")
		}
	})
}
