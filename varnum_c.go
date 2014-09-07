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
#cgo LDFLAGS: -lclntsh

#include <oci.h>
#include <stdio.h>
#include <string.h>
*/
import "C"

import (
	"unsafe"
)

func (n *OCINumber) SetCOCINumber(m C.OCINumber) {
	C.memcpy(unsafe.Pointer(&n[0]), unsafe.Pointer(&m), OciNumberSize)
}
