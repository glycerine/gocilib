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

package main

import (
	"flag"
	"log"

	"github.com/tgulacsi/gocilib"
)

func main() {
	flagConnect := flag.String("connect", "", "DSN to connect to")
	flag.Parse()
	conn, err := gocilib.NewConnection("tgulacsi", "tgulacsi", "XE")
	if err != nil {
		log.Fatalf("error connecting to %s: %v", *flagConnect, err)
	}
	defer conn.Close()
	log.Printf("conn: %#v", conn)

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatalf("cannot create statement: %v", err)
	}
	defer stmt.Close()
	log.Printf("stmt: %#v", stmt)

	log.Printf("Creating tables ...")
	stmt.Execute("DROP TABLE TST_notify")
	if err = stmt.Execute("CREATE TABLE TST_notify (key VARCHAR2(10), value VARCHAR2(1000))"); err != nil {
		log.Fatalf("error creating table TST_notify: %v", err)
	}
	defer stmt.Execute("DROP TABLE TST_notify")

	log.Printf("registering subscription ...")
	sub, err := conn.NewSubscription("sub-00", gocilib.EvtAll)
	if err != nil {
		log.Fatalf("error creating subscription: %v", err)
	}
	log.Printf("sub: %#v", sub)
}
