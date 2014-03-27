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

// #cgo LDFLAGS: -locilib -lclntsh
// #include "ocilib.h"
// #include "oci.h"
// extern OCI_Subscription *subscriptionRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout, boolean rowids_needed);
// sb4 setupNotifications2(OCISubscription **subscrhpp, OCI_Connection *con, ub4 subscriptionID, ub4 operations, boolean rowids_needed, ub4 timeout);
// extern const int RowidLength;
// extern sb4 subsAddStatement2(OCI_Subscription *sub, OCI_Statement *stmt);
import "C"

import (
	"bytes"
	"log"
	"math/rand"
	"strings"
	"sync"
	"unsafe"
)

var rowidLength = int(C.RowidLength)

type EventType int

const (
	EvtAll       = EventType(C.OCI_CNT_ALL)       // request for all changes
	EvtRows      = EventType(C.OCI_CNT_ROWS)      // request for changes at rows level (DML)
	EvtDatabases = EventType(C.OCI_CNT_DATABASES) // request for changes at database level (startup, shutdown)
	EvtObjects   = EventType(C.OCI_CNT_OBJECTS)   // request for changes at objects (eg. tables) level (DDL / DML)
)

type Subscription interface {
	AddStatement(st *Statement) (<-chan Event, error)
	Close() error
}

type libSubscription struct {
	handle *C.OCI_Subscription
	events chan Event
}

type rawSubscription struct {
	handle *C.OCISubscription
	conn   *C.OCI_Connection
	ID     uint32
	events chan Event
}

var (
	subscriptionsMu  sync.Mutex
	libSubscriptions map[*C.OCI_Subscription]*libSubscription
	rawSubscriptions map[uint32]*rawSubscription
)

func (conn *Connection) NewLibSubscription(name string, evt EventType, rowidsNeeded bool, timeout int) (Subscription, error) {
	CrowidsNeeded := C.boolean(C.FALSE)
	if rowidsNeeded {
		CrowidsNeeded = C.TRUE
	}

	subs := libSubscription{
		handle: C.subscriptionRegister(conn.handle, C.CString(name), C.uint(evt),
			0, C.uint(timeout), CrowidsNeeded),
	}
	if subs.handle == nil {
		return nil, getLastErr()
	}
	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	if libSubscriptions == nil {
		libSubscriptions = make(map[*C.OCI_Subscription]*libSubscription, 1)
	}
	libSubscriptions[subs.handle] = &subs
	return &subs, nil
}

// AddStatement adds the statement to be watched, and returns the event channel.
func (subs *libSubscription) AddStatement(st *Statement) (<-chan Event, error) {
	if C.OCI_SubscriptionAddStatement(subs.handle, st.handle) != C.TRUE {
		return nil, getLastErr()
	}
	return subs.events, nil
}

// AddQuery is a conveniance function which prepares the query and adds the statement.
func (subs *libSubscription) AddQuery(conn *Connection, qry string) (<-chan Event, error) {
	stmt, err := conn.NewPreparedStatement(qry)
	if err != nil {
		return nil, err
	}
	return subs.AddStatement(stmt)
}

func (subs *libSubscription) Close() error {
	var err error
	if subs.handle != nil {
		subscriptionsMu.Lock()
		delete(libSubscriptions, subs.handle)
		subscriptionsMu.Unlock()
		if C.OCI_SubscriptionUnregister(subs.handle) != C.TRUE {
			err = getLastErr()
		}
		subs.handle = nil
	}
	return err
}

func getSubscriptionFromHandle(handle *C.OCI_Subscription) Subscription {
	if handle == nil {
		return nil
	}
	subscriptionsMu.Lock()
	subs := libSubscriptions[handle]
	subscriptionsMu.Unlock()
	return subs
}

func (conn *Connection) NewRawSubscription(name string, evt EventType, rowidsNeeded bool, timeout int) (Subscription, error) {
	CrowidsNeeded := C.boolean(C.FALSE)
	if rowidsNeeded {
		CrowidsNeeded = C.TRUE
	}
	subscriptionID := uint32(rand.Int31())

	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()

	var subshp *C.OCISubscription
	if C.setupNotifications2(
		&subshp, conn.handle, C.ub4(subscriptionID), C.ub4(evt),
		CrowidsNeeded, C.ub4(timeout),
	) != C.OCI_SUCCESS {
		return nil, getLastErr()
	}
	subs := rawSubscription{handle: subshp, conn: conn.handle,
		events: make(chan Event, 1), ID: subscriptionID}
	if rawSubscriptions == nil {
		rawSubscriptions = make(map[uint32]*rawSubscription, 1)
	}
	rawSubscriptions[subscriptionID] = &subs
	return subs, nil
}

// AddStatement adds the statement to be watched, and returns the event channel.
func (subs rawSubscription) AddStatement(st *Statement) (<-chan Event, error) {
	rc := C.subsAddStatement2(subs.handle, st.handle)
	if rc != C.TRUE {
		err := getLastErr().(*Error)
		if err.Code == 0 {
			err = getLastRawError(C.OCI_StatementGetConnection(st.handle))
			err.Code = int(rc)
		}
		return nil, err
	}
	return subs.events, nil
}

// AddQuery is a conveniance function which prepares the query and adds the statement.
func (subs rawSubscription) AddQuery(conn *Connection, qry string) (<-chan Event, error) {
	stmt, err := conn.NewPreparedStatement(qry)
	if err != nil {
		return nil, err
	}
	return subs.AddStatement(stmt)
}

func (subs rawSubscription) Close() error {
	var err error
	if subs.handle != nil {
		subscriptionsMu.Lock()
		delete(rawSubscriptions, subs.ID)
		subscriptionsMu.Unlock()
		if C.OCISubscriptionUnRegister(
			(*C.OCISvcCtx)(C.OCI_HandleGetContext(subs.conn)),
			subs.handle,
			(*C.OCIError)(C.OCI_HandleGetError(subs.conn)),
			C.OCI_DEFAULT,
		) != C.OCI_SUCCESS {
			err = getLastErr()
		}
		subs.handle = nil
	}
	return err
}

func getSubscriptionFromID(ID C.uint) rawSubscription {
	subscriptionsMu.Lock()
	subs := rawSubscriptions[uint32(ID)]
	subscriptionsMu.Unlock()
	return *subs
}

type Event struct {
	typ, op int
	rowid   string
}

//export goNotificationCallback
func goNotificationCallback(subscriptionID C.uint, notifyType C.uint,
	table_name *C.char, rows *C.char, num_rows C.int,
) {
	log.Printf("CALLBACK ID=%d type=%d", subscriptionID, notifyType)
	if table_name == nil {
		return
	}
	table := C.GoString(table_name)
	if rows == nil {
		return
	}
	if num_rows <= 0 {
		return
	}
	all := C.GoStringN(rows, num_rows*C.int(rowidLength))
	rowids := make([]string, int(num_rows))
	for i := range rowids {
		rowids[i] = all[i*rowidLength : (i+1)*rowidLength]
	}
	log.Printf("CALLBACK type=%d table=%s rowids=%q", notifyType, table, rowids)
}

//export goEventHandler
func goEventHandler(eventP unsafe.Pointer) {
	log.Printf("EVENT %p", eventP)
	event := (*C.OCI_Event)(eventP)
	typ := C.OCI_EventGetType(event)
	op := C.OCI_EventGetOperation(event)
	handle := C.OCI_EventGetSubscription(event)

	subs := getSubscriptionFromHandle(handle).(*libSubscription)
	switch typ {
	case C.OCI_ENT_DEREGISTER:
		subs.events <- Event{typ: C.OCI_ENT_DEREGISTER}
	case C.OCI_ENT_STARTUP, C.OCI_ENT_SHUTDOWN, C.OCI_ENT_SHUTDOWN_ANY, C.OCI_ENT_DROP_DATABASE:
		subs.events <- Event{typ: int(typ)}
	case C.OCI_ENT_OBJECT_CHANGED:
		//object := C.OCI_EventGetObject(event)
		switch op {
		case C.OCI_ONT_INSERT, C.OCI_ONT_UPDATE, C.OCI_ONT_DELETE:
			subs.events <- Event{typ: int(typ), op: int(op),
				rowid: C.GoString(C.OCI_EventGetRowid(event))}
		default:
			subs.events <- Event{typ: int(typ), op: int(op)}
		}
	}
}

func getLastRawError(con *C.OCI_Connection) *Error {
	errbuf := make([]byte, 4000)
	var (
		i, errorcode int
		ec           C.sb4
		message      []string
	)
	conp := unsafe.Pointer(C.OCI_HandleGetError(con))
	for {
		i++
		errstat := C.OCIErrorGet(conp, C.ub4(i), nil,
			&ec, (*C.OraText)(&errbuf[0]), C.ub4(len(errbuf)-1),
			C.OCI_HTYPE_ERROR)
		if ec != 0 && errorcode == 0 {
			errorcode = int(ec)
		}
		if errstat == C.OCI_NO_DATA || i > 100 {
			break
		}
		message = append(message, string(errbuf[:bytes.IndexByte(errbuf, 0)]))
	}
	return &Error{Code: errorcode, Text: strings.Join(message, "")}
}
