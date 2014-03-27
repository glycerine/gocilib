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
#include <stdlib.h>
#include <string.h>
#include <oci.h>

#define MAXSTRLENGTH 1024

static void checker();


void checker(errhp, status)
OCIError *errhp;
sword status;
{
  text errbuf[512];
  sb4 errcode = 0;
  int retval = 1;

  switch (status)
  {
  case OCI_SUCCESS:
    retval = 0;
    break;
  case OCI_SUCCESS_WITH_INFO:
    (void) printf("Error - OCI_SUCCESS_WITH_INFO\n");
    break;
  case OCI_NEED_DATA:
    (void) printf("Error - OCI_NEED_DATA\n");
    break;
  case OCI_NO_DATA:
    (void) printf("Error - OCI_NODATA\n");
    break;
  case OCI_ERROR:
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    (void) printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    (void) printf("Error - OCI_INVALID_HANDLE\n");
    break;
  case OCI_STILL_EXECUTING:
    (void) printf("Error - OCI_STILL_EXECUTE\n");
    break;
  case OCI_CONTINUE:
    (void) printf("Error - OCI_CONTINUE\n");
    break;
  default:
    break;
  }
}

void notification_callback(ctx, subscrhp, payload, payl, descriptor, mode)
dvoid *ctx;
OCISubscription *subscrhp;
dvoid *payload;
ub4 *payl;
dvoid *descriptor;
ub4 mode;
{
  dvoid *change_descriptor =  descriptor;
  ub4   notify_type;
  OCIEnv *envhp;
  OCIError *errhp;
  OCIServer *srvhp;
  OCISvcCtx *svchp;
  OCISession *usrhp;
  OCIStmt *stmthp;
  dvoid *tmp;

  dvoid *elemind = (dvoid *)0;
  OCIColl *table_changes = (OCIColl  *)0 ;
                   /* Collection of pointers to table chg descriptors */
  dvoid **table_descp;          /* Pointer to Table Change Descriptor */
  dvoid *table_desc;              /*  Table Change Descriptor */
  ub4 num_rows = 0;
  ub4 table_op;
  ub4 num_tables = 0;
  ub2 i, j;
  boolean exist;
  text *table_name;

  printf("Received Notification\n");



  /* Initialize environment and allocate Error Handle.
     Note that the environment has to be initialized in object mode
     since we might be operating on collections.
  */
  OCIEnvCreate( (OCIEnv **) &envhp, OCI_OBJECT, (dvoid *)0,
                 (dvoid * (*)(dvoid *, size_t)) 0,
                 (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                 (void (*)(dvoid *, dvoid *)) 0,
                 (size_t) 0, (dvoid **) 0 );

  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR,
                   (size_t) 0, (dvoid **) 0);

  /* Get the Notification Type */
  checker(errhp,
          OCIAttrGet( change_descriptor, OCI_DTYPE_CHDES, &notify_type, NULL,
                OCI_ATTR_CHDES_NFYTYPE, errhp));
  if (notify_type == OCI_EVENT_SHUTDOWN) {
    printf("Shutdown Notification\n");
    goNotificationCallback(notify_type);
  } else if (notify_type == OCI_EVENT_DEREG) {
    printf("Registration Removed\n");
    goNotificationCallback(notify_type);
  }

  if (notify_type != OCI_EVENT_OBJCHANGE)
  {
    OCIHandleFree((dvoid *)envhp, OCI_HTYPE_ENV);
    OCIHandleFree((dvoid *)errhp, OCI_HTYPE_ERROR);
    return;
  }

  /* The below code is only executed if the notification is of type
     OCI_EVENT_OBJCHANGE
   */
  /* server contexts */
  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &srvhp,
                            OCI_HTYPE_SERVER,
                   (size_t) 0, (dvoid **) 0);
  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp,
                             OCI_HTYPE_SVCCTX,
                              (size_t) 0, (dvoid **) 0);

  /* Allocate a statement handle */
  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp,
                  (ub4) OCI_HTYPE_STMT, 52, (dvoid **) &tmp);

  /* set attribute server context in the service context */
  OCIAttrSet( (dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX, (dvoid *)srvhp,
              (ub4) 0, (ub4) OCI_ATTR_SERVER, (OCIError *) errhp);

  checker(errhp, OCIServerAttach( srvhp, errhp, (text *) 0, (sb4) 0,
          (ub4) OCI_DEFAULT));

   /* allocate a SESSION  handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp, (ub4) OCI_HTYPE_SESSION,
                     (size_t) 0, (dvoid **) 0);

  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
             (dvoid *)((text *)"HR"),
             (ub4)strlen((char *)"HR"),  OCI_ATTR_USERNAME, errhp);

  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
             (dvoid *)((text *)"HR"), (ub4)strlen((char *)"HR"),
             OCI_ATTR_PASSWORD, errhp);

  checker(errhp,OCISessionBegin (svchp, errhp, usrhp, OCI_CRED_RDBMS,
                                 OCI_DEFAULT));
  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX,
             (dvoid *)usrhp, (ub4)0, OCI_ATTR_SESSION, errhp);

  /* Obtain the collection of table change descriptors */
  checker(errhp,OCIAttrGet(change_descriptor, OCI_DTYPE_CHDES, &table_changes,
                           NULL, OCI_ATTR_CHDES_TABLE_CHANGES, errhp));
 /* Obtain the size of the collection (i.e number of tables modified) */
  if (table_changes) {
    checker(errhp, OCICollSize(envhp, errhp, (CONST OCIColl *) table_changes,
            &num_tables));
  } else {
     num_tables = 0;
  }

  /* For each element of the collection, extract the table name of the modified
     table */
  for (i = 0; i < num_tables; i++) {
    OCIColl *row_changes = (OCIColl  *)0;
    /* Collection of pointers to row chg. Descriptors */
    dvoid **row_descp;            /*  Pointer to Row Change Descriptor */
    dvoid   *row_desc;               /*   Row Change Descriptor */
    text *row_id;
    ub4 rowid_size;
    text *ocistmt;
    OCIDefine *defnp1 = (OCIDefine *)0;
    char *outstr;

    checker(errhp,OCICollGetElem(envhp, errhp, (OCIColl *) table_changes, i,
                                 &exist, &table_descp, &elemind));

    table_desc = *table_descp;
    checker(errhp,OCIAttrGet(table_desc, OCI_DTYPE_TABLE_CHDES, &table_name,
                             NULL,
                             OCI_ATTR_CHDES_TABLE_NAME, errhp));
    if (strcmp(table_name, "HR.EMPLOYEES") == 0) {
      printf("EMPLOYEE table modified \n");
    } else if (strcmp(table_name, "HR.DEPARTMENTS") == 0) {
      printf("DEPARTMENTS table modified \n");
    }

    checker(errhp,OCIAttrGet (table_desc, OCI_DTYPE_TABLE_CHDES,
                              (dvoid *)&table_op, NULL,
                               OCI_ATTR_CHDES_TABLE_OPFLAGS, errhp));

    /* If the ROWID granularity of info not available, move-on. Rowids
       can be rolled up into a full table notification if too many rows
       were updated on a single table or insufficient shared memory on
       the server side to hold rowids
     */
    if (table_op & OCI_OPCODE_ALLROWS) {
      printf("Full Table Invalidation\n");
      continue;
    }

     /* Obtain the collection of  ROW CHANGE descriptors */
    checker(errhp,OCIAttrGet (table_desc, OCI_DTYPE_TABLE_CHDES, &row_changes,
                               NULL, OCI_ATTR_CHDES_TABLE_ROW_CHANGES, errhp));

    if (row_changes) {
      checker(errhp,OCICollSize(envhp, errhp, row_changes, &num_rows));
    } else {
      num_rows = 0;
    }

    printf ("Number of rows modified is %d\n", num_rows);
    fflush(stdout);
    for (j = 0; j<num_rows; j++) {
      OCICollGetElem(envhp, errhp, (OCIColl *) row_changes,
                     j, &exist, &row_descp, &elemind);
      row_desc = *row_descp;

      OCIAttrGet (row_desc, OCI_DTYPE_ROW_CHDES, (dvoid *)&row_id,
                  &rowid_size, OCI_ATTR_CHDES_ROW_ROWID, errhp);
      printf ("%s table has been modified in row %s \n", table_name, row_id);
      fflush(stdout);
      ocistmt = (text *)malloc(MAXSTRLENGTH*sizeof(char));

      /* QUERY FROM DATABASE TO VIEW CONTENTS OF CHANGED ROW */
      sprintf (ocistmt, "select * from %s where rowid='%s'", table_name, row_id);
      printf("Executing stmt %s\n", ocistmt);

      /* prepare query statement*/
      checker(errhp,OCIStmtPrepare(stmthp, errhp, ocistmt,
                                   (ub4)strlen((char *)ocistmt),
                                   (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));
      outstr = (char *)malloc(MAXSTRLENGTH*sizeof(char));

      checker(errhp,OCIDefineByPos(stmthp, &defnp1, errhp, 1, (dvoid *)outstr,
                                   MAXSTRLENGTH * sizeof(char),
                                   SQLT_STR, (dvoid *)0, (ub2 *)0, (ub2 *)0,
                                   OCI_DEFAULT));

      /* execute the statement */
      checker(errhp,OCIStmtExecute(svchp, stmthp, errhp, (ub4)1, (ub4) 0,
                                   (CONST OCISnapshot *) NULL,
                                   (OCISnapshot *) NULL, OCI_DEFAULT));

      printf("First column of modified row is %s\n", outstr);
    }  /* Loop for j in 1..numrows */

  } /* Loop for I in 1..numtables */

  /* End session and detach from server */
  checker(errhp, OCISessionEnd(svchp, errhp, usrhp, OCI_DEFAULT));
  checker(errhp, OCIServerDetach(srvhp, errhp, OCI_DEFAULT));
  if (stmthp)
    OCIHandleFree((dvoid *)stmthp, OCI_HTYPE_STMT);
  if (errhp)
    OCIHandleFree((dvoid *)errhp, OCI_HTYPE_ERROR);
  if (srvhp)
    OCIHandleFree((dvoid *)srvhp, OCI_HTYPE_SERVER);
  if (svchp)
    OCIHandleFree((dvoid *)svchp, OCI_HTYPE_SVCCTX);
  if (usrhp)
    OCIHandleFree((dvoid *)usrhp, OCI_HTYPE_SESSION);
  if (envhp)
    OCIHandleFree((dvoid *)envhp, OCI_HTYPE_ENV);

}  /* End function notification_callback */


/* The following routine creates registrations and waits for notifications. */
void setupNotifications(envhp, errhp)
{
  OCISvcCtx *svchp;
  OCIError *errhp;
  OCISession *usrhp;
  OCIStmt *stmthp;
  OCIEnv *envhp;
  OCIServer *srvhp;
  text *username;
  OCISubscription *subscrhp;
  ub4 namespace = OCI_SUBSCR_NAMESPACE_DBCHANGE;
  ub4 timeout = 1800;
  text dname[MAXSTRLENGTH];
  OCIDefine *defnp1 = (OCIDefine *)0;
  OCIDefine *defnp2 = (OCIDefine *)0;
  OCIDefine *defnp3 = (OCIDefine *)0;
  OCIDefine *defnp4 = (OCIDefine *)0;
  OCIDefine *defnp5 = (OCIDefine *)0;
  int mgr_id =0;
  int dept_id =0;
  dvoid *tmp;
  boolean rowids_needed = TRUE;

  char query_text1[] = "SELECT MANAGER_ID from EMPLOYEES where EMPLOYEE_ID=206";
  char query_text2[] =
  "SELECT department_id  from DEPARTMENTS where DEPARTMENT_NAME = 'Payroll'";
  /* DEPARTMENTS  is also a cached object */

  /* Initialize the environment. The environment has to be initialized
     with OCI_EVENTS and OCI_OBJECTS to create a change notification
     registration and receive notifications.
  */
  OCIEnvCreate( (OCIEnv **) &envhp, OCI_EVENTS|OCI_OBJECT, (dvoid *)0,
                    (dvoid * (*)(dvoid *, size_t)) 0,
                    (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                    (void (*)(dvoid *, dvoid *)) 0,
                    (size_t) 0, (dvoid **) 0 );

  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR,
                         (size_t) 0, (dvoid **) 0);
   /* server contexts */
  OCIHandleAlloc((dvoid *) envhp, (dvoid **) &srvhp, OCI_HTYPE_SERVER,
                        (size_t) 0,
                        (dvoid **) 0);
  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX,
                         (size_t) 0, (dvoid **) 0);
   checker(errhp,OCIServerAttach( srvhp, errhp, (text *) 0, (sb4) 0,
           (ub4) OCI_DEFAULT));
   OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, (ub4) OCI_HTYPE_SVCCTX,
                   52, (dvoid **)0);
  /* set attribute server context in the service context */
  OCIAttrSet( (dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX, (dvoid *)srvhp,
              (ub4) 0, (ub4) OCI_ATTR_SERVER, (OCIError *) errhp);

   /* allocate a user context handle */
  OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp, (ub4) OCI_HTYPE_SESSION,
                               (size_t) 0, (dvoid **) 0);

  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
             (dvoid *)((text *)"HR"), (ub4)strlen((char *)"HR"),
              OCI_ATTR_USERNAME, errhp);

  OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
            (dvoid *)((text *)"HR"), (ub4)strlen((char *)"HR"),
             OCI_ATTR_PASSWORD, errhp);
  checker(errhp,OCISessionBegin (svchp, errhp, usrhp, OCI_CRED_RDBMS,
           OCI_DEFAULT));
   /* Allocate a statement handle */
  OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp,
                                (ub4) OCI_HTYPE_STMT, 52, (dvoid **) &tmp);

  OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)usrhp, (ub4)0,
                       OCI_ATTR_SESSION, errhp);

  /* allocate subscription handle */
  OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &subscrhp, OCI_HTYPE_SUBSCRIPTION,
                   (size_t) 0,
                    (dvoid **) 0);
  printf("Allocated handles\n");

  /* set the namespace to DBCHANGE */
  OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,  (dvoid *) &namespace,
              sizeof(ub4),
              OCI_ATTR_SUBSCR_NAMESPACE, errhp);
  /* Associate a notification callback */
  OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
              (void *)notification_callback,  0,
              OCI_ATTR_SUBSCR_CALLBACK, errhp);

  /* Allow extraction of rowid information */
  checker(errhp, OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
                  (dvoid *)&rowids_needed, sizeof(ub4),
                  OCI_ATTR_CHNF_ROWIDS, errhp));

  /* Can optionally provide a client specific context using
     OCI_ATTR_SUBSCR_CTX */

  /* Set a timeout value of half an hour */
  OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
             (dvoid *)&timeout, 0, OCI_ATTR_SUBSCR_TIMEOUT, errhp);

  /* Create a new registration in the  DBCHANGE namespace */
  checker(errhp,OCISubscriptionRegister(svchp, &subscrhp, 1, errhp, OCI_DEFAULT));

  printf("Created Registration\n");
/* Multiple queries can now be associated with the subscription */

  /* Prepare the statement */
  checker(errhp,OCIStmtPrepare (stmthp, errhp, query_text1,
                                strlen(query_text1), OCI_V7_SYNTAX, OCI_DEFAULT));
  checker(errhp,OCIDefineByPos(stmthp, &defnp1, errhp, 1, (dvoid *)&mgr_id,
                               sizeof(mgr_id), SQLT_INT, (dvoid *)0,
                               (ub2 *)0, (ub2 *)0, OCI_DEFAULT));
  /* Associate the statement with the subscription handle */
  checker(errhp,OCIAttrSet (stmthp, OCI_HTYPE_STMT, subscrhp, 0,
                            OCI_ATTR_CHNF_REGHANDLE, errhp));

  /* Execute the statement The execution of the statement  performs the object
  registration */
  checker(errhp,OCIStmtExecute (svchp, stmthp, errhp, (ub4) 1, (ub4) 0,
                 (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL ,
                 OCI_DEFAULT));
  printf("Registered query %s\n", query_text1);

  /* Use the same registration for the departments table */
  checker(errhp,OCIStmtPrepare (stmthp, errhp, query_text2,
                strlen(query_text2), OCI_V7_SYNTAX, OCI_DEFAULT));

  checker(errhp,OCIDefineByPos(stmthp, &defnp3,
               errhp, 1, (dvoid *)&dept_id, sizeof(dept_id),
                SQLT_INT, (dvoid *)0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT));
  checker(errhp,OCIAttrSet (stmthp, OCI_HTYPE_STMT, subscrhp, 0,
                     OCI_ATTR_CHNF_REGHANDLE, errhp));
  checker(errhp,OCIStmtExecute (svchp, stmthp, errhp, (ub4) 1, (ub4) 0,
                 (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL ,
                 OCI_DEFAULT));
  printf("Registered query %s\n", query_text2);

  printf("Waiting for Notifications to arrive\n");
  /* Wait for notifications to arrive */
  while (notifications_processed != 1);

  /* Unregister the subscription */
  checker(errhp,
           OCISubscriptionUnRegister(svchp,subscrhp, errhp, OCI_DEFAULT));

  /* End the session and detach from the server */
  checker(errhp, OCISessionEnd(svchp, errhp, usrhp, (ub4) 0));
  checker(errhp, OCIServerDetach(srvhp, errhp, (ub4) OCI_DEFAULT));

  /* Free all the handles */
  OCIHandleFree((dvoid *)subscrhp, OCI_HTYPE_SUBSCRIPTION);
  OCIHandleFree((dvoid *)stmthp, OCI_HTYPE_STMT);
  OCIHandleFree((dvoid *) srvhp, (ub4) OCI_HTYPE_SERVER);
  OCIHandleFree((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX);
  OCIHandleFree((dvoid *) usrhp, (ub4) OCI_HTYPE_SESSION);
  OCIHandleFree((dvoid *) errhp, (ub4) OCI_HTYPE_ERROR);
  OCIHandleFree((dvoid *) envhp, (ub4) OCI_HTYPE_ENV);
}
