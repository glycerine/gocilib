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

/*
#cgo LDFLAGS: -locilib -lclntsh
#include "ocilib.h"
#include "oci.h"

extern OCI_Subscription *libSubsRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout, boolean rowids_needed);

extern const int RowidLength;
*/
import "C"

import (
	"bytes"
	"errors"
	"log"
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
	name   string
	events chan Event
}

var (
	subscriptionsMu  sync.Mutex
	libSubscriptions map[string]*libSubscription
)

func (conn *Connection) NewLibSubscription(name string, evt EventType, rowidsNeeded bool, timeout int) (Subscription, error) {
	subscriptionsMu.Lock()
	defer subscriptionsMu.Unlock()
	if libSubscriptions == nil {
		libSubscriptions = make(map[string]*libSubscription, 1)
	}
	if _, ok := libSubscriptions[name]; ok {
		return nil, errors.New("Subscription " + name + " already registered.")
	}

	CrowidsNeeded := C.boolean(C.FALSE)
	if rowidsNeeded {
		CrowidsNeeded = C.TRUE
	}

	subs := libSubscription{
		name: name,
		handle: C.libSubsRegister(conn.handle, C.CString(name), C.uint(evt),
			0, C.uint(timeout), CrowidsNeeded),
	}
	if subs.handle == nil {
		return nil, getLastErr()
	}

	libSubscriptions[name] = &subs
	return &subs, nil
}

// AddStatement adds the statement to be watched, and returns the event channel.
func (subs *libSubscription) AddStatement(st *Statement) (<-chan Event, error) {
	if C.OCI_SubscriptionAddStatement(subs.handle, st.handle) != C.TRUE {
		return nil, getLastErr()
	}
	if subs.events == nil {
		subs.events = make(chan Event, 1)
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
		delete(libSubscriptions, subs.name)
		subscriptionsMu.Unlock()
		if C.OCI_SubscriptionUnregister(subs.handle) != C.TRUE {
			err = getLastErr()
		}
		subs.handle = nil
	}
	return err
}

func getSubscriptionFromName(name string) *libSubscription {
	if name == "" {
		return nil
	}
	subscriptionsMu.Lock()
	subs := libSubscriptions[name]
	subscriptionsMu.Unlock()
	return subs
}

type Event struct {
	Type, Op                int
	Database, Object, RowID string
}

//export goNotificationCallback
func goNotificationCallback(Cname *C.char, notifyType, op C.uint, Cdatabase, Cobject, Crowid *C.char) {
	name := C.GoString(Cname)
	log.Printf("CALLBACK NAME=%s type=%d", name, notifyType)

	subs := getSubscriptionFromName(name)
	if subs == nil || subs.name == "" {
		log.Printf("Cannot find subscription name %q.", name)
		return
	}
	evt := Event{Type: int(notifyType), Op: int(op), Database: C.GoString(Cdatabase)}
	ok := false
	switch notifyType {
	case C.OCI_ENT_DEREGISTER:
		ok = true
	case C.OCI_ENT_STARTUP, C.OCI_ENT_SHUTDOWN, C.OCI_ENT_SHUTDOWN_ANY, C.OCI_ENT_DROP_DATABASE:
		ok = true
	case C.OCI_ENT_OBJECT_CHANGED:
		//object := C.OCI_EventGetObject(event)
		ok = true
		evt.Object = C.GoString(Cobject)
		switch op {
		case C.OCI_ONT_INSERT, C.OCI_ONT_UPDATE, C.OCI_ONT_DELETE:
			evt.RowID = C.GoString(Crowid)
		}
	}
	if !ok {
		return
	}
	select {
	case subs.events <- evt:
	default:
		log.Printf("WARN: cannot send event %v.", evt)
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
