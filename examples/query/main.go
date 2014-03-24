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
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tgulacsi/gocilib"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] query\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(3)
	}
	flagConnect := flag.String("connect", "", "DSN to connect to")
	flagCsv := flag.Bool("csv", false, "CSV output")
	flag.Parse()

	qry := strings.Join(flag.Args(), " ")
	if qry == "" {
		flag.Usage()
	}

	user, passwd, sid := gocilib.SplitDSN(*flagConnect)
	conn, err := gocilib.NewConnection(user, passwd, sid)
	if err != nil {
		log.Fatalf("error connecting to %s: %v", *flagConnect, err)
	}
	defer conn.Close()
	log.Printf("conn: %#v", conn)

	st, err := conn.NewStatement()
	if err != nil {
		log.Fatalf("cannot create statement: %v", err)
	}
	defer st.Close()
	log.Printf("stmt: %#v", st)

	if err = st.Execute(qry); err != nil {
		log.Fatalf("error executing %q: %v", qry, err)
	}
	_ = *flagCsv

}
