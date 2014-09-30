#include <ocilib.h>
#include <stdio.h>
#include <stdlib.h>
#include <oci.h>

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

int main(int argc, char *argv[])
{
  OCI_Connection* cn;
  OCI_Statement* st;
  OCI_Resultset* rs;
  OCI_Error*     err;

  OCIError* errhp;
  OCINumber in_nums[3];
  char out_txt[32767];
  char tmp[255];

  if(!OCI_Initialize(NULL, NULL, OCI_ENV_DEFAULT|OCI_ENV_THREADED|OCI_ENV_CONTEXT|OCI_ENV_EVENTS)) {
      return EXIT_FAILURE;
  }

  cn = OCI_ConnectionCreate("XE", "tgulacsi", "tgulacsi", OCI_SESSION_DEFAULT);
  printf("cn=%p\n", cn);

  st = OCI_StatementCreate(cn);
  printf("st=%p\n", cn);
  if(!OCI_Prepare(st, "DECLARE\nTYPE num_tab_typ IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;\n tab num_tab_typ := :1;\n v_idx PLS_INTEGER;\n v_txt VARCHAR2(1000);\n BEGIN\n v_idx := tab.FIRST;\n WHILE v_idx IS NOT NULL LOOP\n v_txt := v_txt||v_idx||'='||tab(v_idx)||';';\n v_idx := tab.NEXT(v_idx);\n END LOOP;\n :2 := v_txt;\n END;")) {
	  return EXIT_FAILURE;
  }
  errhp = (OCIError*)OCI_HandleGetError(cn);
  printf("errhp=%p\n", errhp);
  int n = 255;
  int i;
  for(i=0; i < 3; i++) {
    NumberFromDouble(errhp, &(in_nums[i]), i+1.0);
    n = 255; NumberToText(errhp, tmp, &n, &in_nums[i]); printf("in_nums[%d]=%s\n", i, tmp);
  }
  printf("in_nums=%s\n", in_nums);
  OCI_BindArraySetSize(st, 1000);
  printf("setSize\n");
  OCI_BindArrayOfNumbers(st, ":1", in_nums, 3);
  printf("bindArray\n");
  OCI_BindString(st, ":2", out_txt, 32767);
  printf("bindString\n");

  if(!OCI_Execute(st)) {
	printf("ERROR\n");
	err = OCI_GetBatchError(st);

	while (err)
	  {
		printf("Error at row %d : %s\n", OCI_ErrorGetRow(err), OCI_ErrorGetString(err));

		err = OCI_GetBatchError(st);
	  }
  } else {
	printf("OK\n");
	printf("out=%s\n", out_txt);
  }

  OCI_Cleanup();

  return EXIT_SUCCESS;
}
