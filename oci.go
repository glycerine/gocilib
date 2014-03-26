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

//#cgo LDFLAGS: -lclntsh
//
//#include <stdlib.h>
//#include <oci.h>
//
//const int sizeof_OraText = sizeof(OraText);
//
import "C"

import (
	"unsafe"
)

// getBindInfo returns the bind names, using *C.OCIStmt and *C.OCIError handles.
func getBindInfo(stmtHandle, errHandle unsafe.Pointer, dst []string) ([]string, error) {
	var foundElements C.sb4
	numElements := 8

	for i := 0; i < 2; i++ {
		// avoid bus errors on 64-bit platforms
		// numElements = numElements + (sizeof(void*) - numElements % sizeof(void*));

		// initialize the buffers
		// buffer := make([]byte, numElements*int(C.bindInfo_elementSize))
		// bindNames := (**C.OraText)(unsafe.Pointer(&buffer[0]))
		bindNames := make([](*C.OraText), numElements)
		// bindNameLengths := (*C.ub1)(&buffer[0+numElements])
		bindNameLengths := make([]byte, numElements)
		// indicatorNames := (**C.OraText)(unsafe.Pointer(&buffer[1*numElements+numElements]))
		indicatorNames := make([](*C.OraText), numElements)
		// indicatorNameLengths := (*C.ub1)(&buffer[2*numElements+numElements])
		indicatorNameLengths := make([]byte, numElements)
		// duplicate := (*C.ub1)(unsafe.Pointer(&buffer[3*numElements+numElements]))
		duplicate := make([]byte, numElements)
		// bindHandles := (**C.OCIBind)(unsafe.Pointer(&buffer[4*numElements+numElements]))
		bindHandles := make([](*C.OCIBind), numElements)

		// get the bind information
		status := C.OCIStmtGetBindInfo((*C.OCIStmt)(stmtHandle),
			(*C.OCIError)(errHandle), C.ub4(numElements), 1, &foundElements,
			(**C.OraText)(unsafe.Pointer(&bindNames[0])),
			(*C.ub1)(&bindNameLengths[0]),
			(**C.OraText)(&indicatorNames[0]), (*C.ub1)(&indicatorNameLengths[0]),
			(*C.ub1)(&duplicate[0]), (**C.OCIBind)(&bindHandles[0]))
		if status != C.OCI_NO_DATA && status != C.OCI_SUCCESS {
			// FIXME(tgulacsi): check that getLastErr works here
			return nil, getLastErr()
		}
		if foundElements < 0 {
			// try again
			numElements = -int(foundElements)
			continue
		}

		// create the list which is to be returned
		if dst != nil {
			dst = dst[:0]
		} else {
			dst = make([]string, 0, foundElements)
		}
		// process the bind information returned
		for i := 0; i < int(foundElements); i++ {
			if duplicate[i] > 0 {
				continue
			}
			dst = append(dst, C.GoStringN((*C.char)(unsafe.Pointer(bindNames[i])), C.int(bindNameLengths[i])))
		}
		break
	}
	return dst, nil
}
