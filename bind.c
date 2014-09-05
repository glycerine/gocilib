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

#define OCI_NUM_NUMBER 0

boolean OCI_API OCI_BindNumber
(
    OCI_Statement *stmt,
    const mtext   *name,
    OCINumber     *data
)
{
    //OCI_CHECK_BIND_CALL2(stmt, name, data, OCI_IPC_BIGINT);

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
    //OCI_CHECK_BIND_CALL2(stmt, name, data, OCI_IPC_BIGINT);

    return OCI_BindData(stmt, data, sizeof(OCINumber), name, OCI_CDT_NUMERIC,
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

    //OCI_CALL2
    //(
      //  res, con,

    printf("\n\nnum=%.03f\n\n", src);
    status = OCINumberFromReal(err, &src, sizeof(double), dst)
    ;//)
    if(status == 0) {
        OCINumberToReal(err, dst, sizeof(double), &check);
        printf("\n check: %.03f\n", check);
    }
    return status == 0;
}
