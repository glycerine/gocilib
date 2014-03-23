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
// extern OCI_Subscription *subscriptionRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout);
import "C"

import (
	"sync"
	"unsafe"
)

type EventType int

const (
	EvtAll       = EventType(C.OCI_CNT_ALL)       // request for all changes
	EvtRows      = EventType(C.OCI_CNT_ROWS)      // request for changes at rows level (DML)
	EvtDatabases = EventType(C.OCI_CNT_DATABASES) // request for changes at database level (startup, shutdown)
	EvtObjects   = EventType(C.OCI_CNT_OBJECTS)   // request for changes at objects (eg. tables) level (DDL / DML)
)

type Subscription struct {
	handle *C.OCI_Subscription
	events chan Event
}

var subscriptionsMu sync.Mutex
var subscriptions map[*C.OCI_Subscription]*Subscription

func (conn *Connection) NewSubscription(name string, evt EventType) (*Subscription, error) {
	port := 0
	subs := Subscription{
		handle: C.subscriptionRegister(conn.handle, C.CString(name), C.uint(evt), C.uint(port), 5),
	}
	if subs.handle == nil {
		return nil, getLastErr()
	}
	subs.events = make(chan Event, 1)
	subscriptionsMu.Lock()
	if subscriptions == nil {
		subscriptions = make(map[*C.OCI_Subscription]*Subscription, 1)
	}
	subscriptions[subs.handle] = &subs
	subscriptionsMu.Unlock()
	return &subs, nil
}

func (subs *Subscription) Unregister() error {
	var err error
	if subs.handle != nil {
		if C.OCI_SubscriptionUnregister(subs.handle) != C.TRUE {
			err = getLastErr()
		}
		subscriptionsMu.Lock()
		delete(subscriptions, subs.handle)
		subscriptionsMu.Unlock()
		subs.handle = nil
	}
	return err
}

func (subs *Subscription) Close() error {
	return subs.Unregister()
}

func getSubscriptionFromHandle(handle *C.OCI_Subscription) *Subscription {
	if handle == nil {
		return nil
	}
	subscriptionsMu.Lock()
	subs := subscriptions[handle]
	subscriptionsMu.Unlock()
	return subs
}

type Event struct {
	typ, op int
	rowid   string
}

//export goEventHandler
func goEventHandler(eventP unsafe.Pointer) {
	event := (*C.OCI_Event)(eventP)
	typ := C.OCI_EventGetType(event)
	op := C.OCI_EventGetOperation(event)
	handle := C.OCI_EventGetSubscription(event)

	subs := getSubscriptionFromHandle(handle)
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
