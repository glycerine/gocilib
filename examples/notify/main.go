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
	"time"

	"github.com/tgulacsi/gocilib"
)

func main() {
	flagConnect := flag.String("connect", "", "DSN to connect to")
	flag.Parse()

	user, passwd, sid := gocilib.SplitDSN(*flagConnect)
	conn, err := gocilib.NewConnection(user, passwd, sid)
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
	sub, err := conn.NewRawSubscription("sub-00", gocilib.EvtAll, true, 300)
	if err != nil {
		log.Fatalf("error creating subscription: %v", err)
	}
	defer sub.Close()

	log.Printf("sub: %#v", sub)

	st, err := conn.NewStatement()
	if err != nil {
		log.Fatalf("error creating statement: %v", err)
	}
	if err = st.Prepare("SELECT key FROM TST_notify"); err != nil {
		log.Fatalf("error preparing query: %v", err)
	}
	events, err := sub.AddStatement(st)
	if err != nil {
		log.Fatalf("error registering query: %v", err)
	}

	if err = conn.SetAutoCommit(true); err != nil {
		log.Fatalf("error setting autocommit: %v", err)
	}

	go func() {
		log.Printf("Executing some DDL operation ...")
		if err := st.Execute("ALTER TABLE TST_notify ADD dt DATE DEFAULT SYSDATE"); err != nil {
			log.Printf("error altering table: %v", err)
		}
		time.Sleep(1 * time.Second)
		log.Printf("Executing some DML operation ...")
		for i, qry := range []string{
			"INSERT INTO TST_notify (key, value) VALUES ('1', 'AAA')",
			"INSERT INTO TST_notify (key, value) VALUES ('2', 'AAA')",
			"UPDATE TST_notify SET VALUE = 'BBB' WHERE key = '1'",
			"DELETE TST_notify WHERE key = '2'",
		} {
			if err := st.Execute(qry); err != nil {
				log.Printf("%d. ERROR executing %s: %v", i, qry, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	select {
	case <-time.After(30 * time.Second):
		log.Printf("no event received in 5 seconds")
	case event := <-events:
		log.Printf("got event %#v", event)
	}
}
