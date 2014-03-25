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

type LOB struct {
	handle *C.OCI_Lob
}

func (lo LOB) Type() C.uint {
	return C.OCI_LobGetType(lo.handle)
}

type File struct {
	handle *C.OCI_File
}

func (fi File) Type() C.uint {
	return C.OCI_FileGetType(fi.handle)
}

type Object struct {
	handle *C.OCI_Object
}

func (ob Object) Type() *C.OCI_TypeInfo {
	return C.OCI_ObjectGetTypeInfo(ob.handle)
}

type Ref struct {
	handle *C.OCI_Ref
}

func (re Ref) Type() *C.OCI_TypeInfo {
	return C.OCI_RefGetTypeInfo(re.handle)
}

type Coll struct {
	handle *C.OCI_Coll
}

func (co Coll) Type() *C.OCI_TypeInfo {
	return C.OCI_CollGetTypeInfo(co.handle)
}

type Long struct {
	handle *C.OCI_Long
}

func (lo Long) Len() C.uint {
	return C.OCI_LongGetSize(lo.handle)
}
