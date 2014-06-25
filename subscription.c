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
#include <ocilib.h>
#include "_cgo_export.h"

#define ROWID_LENGTH 18
const int RowidLength = ROWID_LENGTH;

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

void raw_notification_callback(ctx, subscrhp, payload, payl, descriptor, mode)
dvoid *ctx;
OCISubscription *subscrhp;
dvoid *payload;
ub4 *payl;
dvoid *descriptor;
ub4 mode;
{
  ub4 subscriptionID = *((ub4 *)ctx);
  dvoid *change_descriptor =  descriptor;
  ub4   notify_type;
  OCIEnv *envhp;
  OCIError *errhp;

  dvoid *elemind = (dvoid *)0;
  OCIColl *table_changes = (OCIColl  *)0 ;
                   /* Collection of pointers to table chg descriptors */
  dvoid *table_desc;              /*  Table Change Descriptor */
  ub4 num_rows = 0;
  ub4 table_op;
  ub4 num_tables = 0;
  ub2 i, j, length;
  boolean exist;
  text *table_name;

  text *rowids;

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
    goNotificationCallback(subscriptionID, notify_type, NULL, "", -1);
  } else if (notify_type == OCI_EVENT_DEREG) {
    printf("Registration Removed\n");
    goNotificationCallback(subscriptionID, notify_type, NULL, "", -1);
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
    dvoid   *row_desc;               /*   Row Change Descriptor */
    ub4 rowid_size;
    OCIDefine *defnp1 = (OCIDefine *)0;

    checker(errhp,OCICollGetElem(envhp, errhp, (OCIColl *) table_changes, i,
                                 &exist, &table_desc, &elemind));

    checker(errhp,OCIAttrGet(table_desc, OCI_DTYPE_TABLE_CHDES, &table_name,
                             NULL,
                             OCI_ATTR_CHDES_TABLE_NAME, errhp));

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
      goNotificationCallback(subscriptionID, notify_type, table_name, NULL, -1);
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

    rowids = (char *)malloc(ROWID_LENGTH * num_rows);
    if (rowids == NULL) {
        printf ("cannot allocate memory for %d rowids", num_rows);
        continue;
    }
    for (j = 0; j < num_rows; j++) {
      OCICollGetElem(envhp, errhp, (OCIColl *) row_changes,
                     j, &exist, &row_desc, &elemind);

      rowid_size = ROWID_LENGTH;
      OCIAttrGet (row_desc, OCI_DTYPE_ROW_CHDES,
                  (dvoid *)(rowids + j*ROWID_LENGTH),
                  &rowid_size, OCI_ATTR_CHDES_ROW_ROWID, errhp);
      rowids[j*ROWID_LENGTH + rowid_size + 1] = 0;
      printf ("%s table has been modified in row %s \n", table_name, (rowids+j*ROWID_LENGTH));
      fflush(stdout);
    }
    goNotificationCallback(subscriptionID, notify_type, table_name, rowids, num_rows);
  }

  if (errhp)
    OCIHandleFree((dvoid *)errhp, OCI_HTYPE_ERROR);
  if (envhp)
    OCIHandleFree((dvoid *)envhp, OCI_HTYPE_ENV);

}  /* End function notification_callback */


/* The following routine creates registrations and waits for notifications. */
sb4 rawSetupNotifications(subscrhpp, envhp, errhp, svchp, usrhp, subscriptionID, operations, rowids_needed, timeout)
OCISubscription **subscrhpp;
OCIEnv *envhp;
OCIError *errhp;
OCISvcCtx *svchp;
OCISession *usrhp;
ub4 subscriptionID;
ub4 operations;
boolean rowids_needed;
ub4 timeout;
{
  const ub4 namespace = OCI_SUBSCR_NAMESPACE_DBCHANGE;
  const ub4 qosflags = OCI_SUBSCR_CQ_QOS_QUERY|OCI_SUBSCR_CQ_QOS_BEST_EFFORT;
  if (operations == 0) operations = OCI_OPCODE_ALLOPS;
  sb4 rc;
  OCISubscription *subscrhp;

  /* Initialize the environment. The environment has to be initialized
     with OCI_EVENTS and OCI_OBJECTS to create a change notification
     registration and receive notifications.
  */

  if ((rc = OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX,
                       (dvoid *)usrhp, (ub4)0, OCI_ATTR_SESSION, errhp)) != OCI_SUCCESS)
    return rc;

  /* allocate subscription handle */
  if ((rc = OCIHandleAlloc ((dvoid *) envhp, (dvoid **) subscrhpp,
                            OCI_HTYPE_SUBSCRIPTION, (size_t) 0, (dvoid **) 0)) != OCI_SUCCESS)
    return rc;
  printf("Allocated handles\n");

  subscrhp = *subscrhpp;

  /* set the namespace to DBCHANGE */
  printf("Set the namespace\n");
  if ((rc = OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
                        (dvoid *) &namespace, sizeof(ub4),
                        OCI_ATTR_SUBSCR_NAMESPACE, errhp)) != OCI_SUCCESS)
    return rc;

  /* Associate a notification callback */
  printf("associate callback\n");
  if ((rc = OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
              (void *)raw_notification_callback,  0,
              OCI_ATTR_SUBSCR_CALLBACK, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  if (rowids_needed) {
      /* Allow extraction of rowid information */
      printf("allow extraction of rowid information\n");
      if ((rc = OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
                      (dvoid *)&rowids_needed, sizeof(ub4),
                      OCI_ATTR_CHNF_ROWIDS, errhp)) != OCI_SUCCESS) {
        OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
        return rc;
      }
  }

  /* Provide a client specific context using OCI_ATTR_SUBSCR_CTX */
  printf("provide context\n");
  if ((rc = OCIAttrSet (subscrhp, OCI_HTYPE_SUBSCRIPTION,
                  (dvoid *)&subscriptionID, sizeof(ub4),
                  OCI_ATTR_SUBSCR_CTX, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  /* Set a timeout value */
  printf("set timeout\n");
  if ((rc = OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
                       (dvoid *)&timeout, 0,
                       OCI_ATTR_SUBSCR_TIMEOUT, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  /* Set a QOSFLAG value */
  printf("set qosflag\n");
  if ((rc = OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
                       (dvoid *)&qosflags, 0,
                       OCI_ATTR_SUBSCR_QOSFLAGS, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  /* Set a PROTO value */
  printf("set proto\n");
  ub1 proto = OCI_SUBSCR_PROTO_HTTP;
  if ((rc = OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
                       (dvoid *)&proto, 0,
                       OCI_ATTR_SUBSCR_RECPTPROTO, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  /* Set a RECPT value */
  printf("set recpt\n");
  const char* recpt = "http://p520:8441";
  if ((rc = OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
                       (dvoid *)recpt, strlen(recpt),
                       OCI_ATTR_SUBSCR_RECPT, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }
  /* Set a operations value */
  printf("set operations\n");
  if ((rc = OCIAttrSet(subscrhp, OCI_HTYPE_SUBSCRIPTION,
                       (dvoid *)&operations, 0,
                       OCI_ATTR_CHNF_OPERATIONS, errhp)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }
  /* Create a new registration in the  DBCHANGE namespace */
  printf("create new registration\n");
  if ((rc = OCISubscriptionRegister(svchp, &subscrhp, 1, errhp, OCI_DEFAULT)) != OCI_SUCCESS) {
    OCIHandleFree((dvoid *) subscrhp, (ub4) OCI_HTYPE_SUBSCRIPTION);
    return rc;
  }

  printf("Created Registration\n");
  return OCI_SUCCESS;
}

void lib_event_handler(OCI_Event *event)
{
    unsigned int type     = OCI_EventGetType(event);
    unsigned int op       = OCI_EventGetOperation(event);
    OCI_Subscription *sub = OCI_EventGetSubscription(event);

    printf("** Notification      : %s\n\n", OCI_SubscriptionGetName(sub));
    printf("...... Database      : %s\n",   OCI_EventGetDatabase(event));

    switch (type)
    {
        case OCI_ENT_STARTUP:
            printf("...... Event         : Startup\n");
            break;
        case OCI_ENT_SHUTDOWN:
            printf("...... Event         : Shutdown\n");
            break;
        case OCI_ENT_SHUTDOWN_ANY:
            printf("...... Event         : Shutdown any\n");
            break;
        case OCI_ENT_DROP_DATABASE:
            printf("...... Event         : drop database\n");
            break;
        case OCI_ENT_DEREGISTER:
            printf("...... Event         : deregister\n");
            break;
         case OCI_ENT_OBJECT_CHANGED:
            
            printf("...... Event         : object changed\n");
            printf("........... Object   : %s\n", OCI_EventGetObject(event));
      
            switch (op)
            {
                case OCI_ONT_INSERT:
                    printf("........... Action   : insert\n");
                    break;
                case OCI_ONT_UPDATE:
                    printf("........... Action   : update\n");
                    break;
                case OCI_ONT_DELETE:
                    printf("........... Action   : delete\n");
                    break;
                case OCI_ONT_ALTER:
                    printf("........... Action   : alter\n");
                    break;
                case OCI_ONT_DROP:
                    printf("........... Action   : drop\n");
                    break;
            }
                    
            if (op < OCI_ONT_ALTER)
                printf("........... Rowid    : %s\n",  OCI_EventGetRowid(event));
        
            break;
    }
    
    printf("\n");
}

OCI_Subscription *libSubsRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout, boolean rowids_needed) {
    return OCI_SubscriptionRegister(conn, name, evt, 
            lib_event_handler,
            port, timeout);
}


sb4 rawSubsAddStatement(errhp, subscrhp, stmthp)
    const OCIError *errhp;
    const OCISubscription *subscrhp;
    const OCIStmt *stmthp;
{
    sb4 rc;
    if ((rc = OCIAttrSet((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT,
            (dvoid *)subscrhp, (ub4)0,
            (ub4)OCI_ATTR_CHNF_REGHANDLE, (OCIError *)errhp)) != OCI_SUCCESS) {
        printf("stmthp=%x, subscrhp=%x, errhp=%x, rc=%d\n", stmthp, subscrhp, errhp, rc);
    }
    return rc;
}


sb4 libSubsAddStatement(sub, stmt)
    OCI_Subscription *sub;
    OCI_Statement *stmt;
{
    sb4 rc;
    printf("stmthp=%x subhp=%x errhp=%x\n",
                    OCI_HandleGetStatement(stmt),
                    OCI_HandleGetSubscription(sub),
                    OCI_HandleGetError(OCI_StatementGetConnection(stmt)));
    if ((rc = rawSubsAddStatement(
                    OCI_HandleGetError(OCI_StatementGetConnection(stmt)),
                    OCI_HandleGetSubscription(sub),
                    OCI_HandleGetStatement(stmt)
                    )) != OCI_SUCCESS) {
        printf("subsAddStatement error\n");
        return rc;
    }
    if (OCI_Execute(stmt) && (OCI_GetResultset(stmt) != NULL))
        return OCI_SUCCESS;
    return -1;
}

/* vim: set et tabstop=2: */
