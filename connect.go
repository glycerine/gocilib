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
	"sync"
)

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
	C.OCI_SetAutoCommit(conn.handle, C.FALSE)
	return &conn, nil
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
	return &ociError{int(C.OCI_ErrorGetOCICode(ociErr)),
		C.GoString(C.OCI_ErrorGetString(ociErr))}
}

type ociError struct {
	Code int
	Text string
}

func (e ociError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Text)
}

var initOnce sync.Once

func initialize() {
	initOnce.Do(func() {
		if C.OCI_Initialize(nil, nil,
			C.OCI_ENV_DEFAULT|C.OCI_ENV_THREADED|C.OCI_ENV_CONTEXT|C.OCI_ENV_EVENTS,
		) != C.TRUE {
			panic("error initializing OCILIB")
		}
	})
}
