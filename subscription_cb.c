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

#include "ocilib.h"
#include <stdio.h>

void eventHandler(OCI_Event *event) {
    printf("eventHandler(%s)\n", event);
    goEventHandler((void*)event);
}

OCI_Subscription *subscriptionRegister(OCI_Connection *conn, const char *name, unsigned int evt, unsigned int port, unsigned int timeout) {
    printf("subscriptionRegister(conn=%s, name=%s, evt=%d, port=%d, timeout=%d, handler=%s)\n",
            conn, name, evt, port, timeout, &eventHandler);
    return OCI_SubscriptionRegister(conn, name, evt, &eventHandler, port, timeout);
}

