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
#include "ocilib.h"

void eventHandler(OCI_Event *event) {
    printf("eventHandler(%x)\n", event);
    goEventHandler((void*)event);
}

void event_handler(OCI_Event *event)
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

    goEventHandler((void*)event);
}

OCI_Subscription *subscriptionRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout) {
    printf("subscriptionRegister(conn=%x, name=%s, evt=%d, port=%d, timeout=%d, handler=%x)\n",
            conn, name, evt, port, timeout, &eventHandler);
    return OCI_SubscriptionRegister(conn, name, evt, &event_handler, port, timeout);
}

/*
void error_handler(OCI_Error *err)
{
    int         err_type = OCI_ErrorGetType(err);
    int         err_code = OCI_ErrorGetOCICode(err);
    const char *err_msg  = OCI_ErrorGetString(err);

    printf("** %s - %s\n", err_type == OCI_ERR_WARNING ? "Warning" : "Error", err_msg);
    if (err_type != OCI_ERR_WARNING)
        goErrorHandler(err_code, err_msg);
}

int initialize() {
    if (!OCI_Initialize(error_handler, NULL, OCI_ENV_EVENTS|OCI_ENV_CONTEXT|OCI_ENV_THREADED))
        return EXIT_FAILURE;
    return 0;
}
*/
