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

// LOB holds the OCI_Lob*.
type LOB struct {
	handle *C.OCI_Lob
}

// Type returns the type of the LOB.
func (lo LOB) Type() C.uint {
	return C.OCI_LobGetType(lo.handle)
}

// File holds an OCI_File*.
type File struct {
	handle *C.OCI_File
}

// Type returns the file's type.
func (fi File) Type() C.uint {
	return C.OCI_FileGetType(fi.handle)
}

// Object holds the object.
type Object struct {
	handle *C.OCI_Object
}

// Type returns the object's OCI_TypeInfo*.
func (ob Object) Type() *C.OCI_TypeInfo {
	return C.OCI_ObjectGetTypeInfo(ob.handle)
}

// Ref holds the reference.
type Ref struct {
	handle *C.OCI_Ref
}

// Type returns the reference's OCI_TypeInfo*.
func (re Ref) Type() *C.OCI_TypeInfo {
	return C.OCI_RefGetTypeInfo(re.handle)
}

// Coll holds the collection.
type Coll struct {
	handle *C.OCI_Coll
}

// Type returns the collection's OCI_TypeInfo*.
func (co Coll) Type() *C.OCI_TypeInfo {
	return C.OCI_CollGetTypeInfo(co.handle)
}

// Long is OCI_Long *
type Long struct {
	handle *C.OCI_Long
}

// Len returns the length of the Long.
func (lo Long) Len() C.uint {
	return C.OCI_LongGetSize(lo.handle)
}
