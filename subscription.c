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

void lib_event_handler(OCI_Event *event)
{
    unsigned int type     = OCI_EventGetType(event);
    unsigned int op       = OCI_EventGetOperation(event);
    OCI_Subscription *sub = OCI_EventGetSubscription(event);
    char *objectName      = NULL;
    char *rowid           = NULL;

    //printf("** Notification      : %s\n\n", OCI_SubscriptionGetName(sub));
    //printf("...... Database      : %s\n",   OCI_EventGetDatabase(event));

    switch (type)
    {
        case OCI_ENT_STARTUP:
            //printf("...... Event         : Startup\n");
            break;
        case OCI_ENT_SHUTDOWN:
            //printf("...... Event         : Shutdown\n");
            break;
        case OCI_ENT_SHUTDOWN_ANY:
            //printf("...... Event         : Shutdown any\n");
            break;
        case OCI_ENT_DROP_DATABASE:
            //printf("...... Event         : drop database\n");
            break;
        case OCI_ENT_DEREGISTER:
            //printf("...... Event         : deregister\n");
            break;
         case OCI_ENT_OBJECT_CHANGED:
            objectName = (char *)OCI_EventGetObject(event);
            
            //printf("...... Event         : object changed\n");
            //printf("........... Object   : %s\n", objectName);
      
            switch (op)
            {
                case OCI_ONT_INSERT:
                    //printf("........... Action   : insert\n");
                    break;
                case OCI_ONT_UPDATE:
                    //printf("........... Action   : update\n");
                    break;
                case OCI_ONT_DELETE:
                    //printf("........... Action   : delete\n");
                    break;
                case OCI_ONT_ALTER:
                    //printf("........... Action   : alter\n");
                    break;
                case OCI_ONT_DROP:
                    //printf("........... Action   : drop\n");
                    break;
            }
                    
            if (op < OCI_ONT_ALTER) {
                rowid = (char *)OCI_EventGetRowid(event);

                //printf("........... Rowid    : %s\n",  rowid);
            }
        
            break;
    }
    //printf("\n");

    goNotificationCallback((char *)OCI_SubscriptionGetName(sub), type, op,
            (char *)OCI_EventGetDatabase(event), objectName, rowid);
    
}

OCI_Subscription *libSubsRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout, boolean rowids_needed) {
    return OCI_SubscriptionRegister(conn, name, evt, 
            lib_event_handler,
            port, timeout);
}

/* vim: set et tabstop=2: */
