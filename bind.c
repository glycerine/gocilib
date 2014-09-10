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

#include <stdio.h>
//#include <stdlib.h>
//#include <ocilib.h>
//#include <oci.h>
#include "_cgo_export.h"

/* --------------------------------------------------------------------------------------------- *
* OCI_BindNumber
* --------------------------------------------------------------------------------------------- */

#define OCI_BIND_INPUT 1
#define OCI_BIND_OUTPUT 2

const int OCI_NUM_NUMBER = 0;

boolean OCI_API OCI_BindNumber
(
    OCI_Statement *stmt,
    const mtext   *name,
	OCINumber     *data
)
{
    return OCI_BindData(stmt, data, sizeof(OCINumber), name, OCI_CDT_NUMERIC,
        SQLT_VNU, OCI_BIND_INPUT, OCI_NUM_NUMBER, NULL, 0);
}

boolean OCI_API OCI_BindArrayOfNumbers
(
    OCI_Statement *stmt,
    const mtext   *name,
    OCINumber     *data,
    unsigned int  nbelem
)
{
    return OCI_BindData(stmt, data, sizeof(OCINumber), name, OCI_CDT_NUMERIC,
        SQLT_VNU, OCI_BIND_INPUT, OCI_NUM_NUMBER, NULL, nbelem);
}

boolean OCI_API OCI_BindArrayOfNumbers2
(
    OCI_Statement *stmt,
    const mtext   *name,
    OCINumber     **data,
    unsigned int  nbelem
)
{
    return OCI_BindData(stmt, data, sizeof(OCINumber*), name, OCI_CDT_NUMERIC,
        SQLT_VNU, OCI_BIND_INPUT, OCI_NUM_NUMBER, NULL, nbelem);
}

boolean OCI_API NumberFromDouble
(
    OCIError *err,
    OCINumber *dst,
    double    src
)
{
    boolean res;
    sword status;
    double check;

    status = OCINumberFromReal(err, &src, sizeof(double), dst);
    return status == 0;
}

boolean OCI_API NumberToText
(
    OCIError  *err,
	char      *dst,
	ub4       *dst_size,
    OCINumber *src
)
{
    boolean res;
    sword status;
    double check;

    status = OCINumberToText(err, src, "TM9", 3, NULL, 0, dst_size, dst);
    return status == 0;
}
