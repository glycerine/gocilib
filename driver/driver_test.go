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

package driver

import (
	"database/sql"
	"flag"
	"testing"
)

var fDsn = flag.String("dsn", "", "Oracle DSN")

func TestSimple(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	var (
		err error
		dst interface{}
	)
	for i, qry := range []string{
		"SELECT 1 FROM DUAL",
		"SELECT 12.34 FROM DUAL",
		"SELECT 1234567890123456789012 FROM DUAL",
		"SELECT ROWNUM FROM DUAL",
		"SELECT LOG(10, 2) FROM DUAL",
		"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL",
		"SELECT HEXTORAW('00') FROM DUAL",
		"SELECT TO_DATE('2006-05-04 15:07:08', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL",
		"SELECT NULL FROM DUAL",
		"SELECT TO_CLOB('árvíztűrő tükörfúrógép') FROM DUAL",
	} {
		row := conn.QueryRow(qry)
		if err = row.Scan(&dst); err != nil {
			t.Errorf("%d. error with %q test: %s", i, qry, err)
			t.FailNow()
		}
		t.Logf("%d. %q result: %#v", i, qry, dst)
	}

	qry := "SELECT rn, CHR(rn) FROM (SELECT ROWNUM rn FROM all_objects WHERE ROWNUM < 256)"
	rows, err := conn.Query(qry)
	if err != nil {
		t.Errorf("error with multirow test, query %q: %s", qry, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Errorf("error getting columns for %q: %s", qry, err)
	}
	t.Logf("columns for %q: %v", qry, cols)
	var (
		num int
		str string
	)
	for rows.Next() {
		if err = rows.Scan(&num, &str); err != nil {
			t.Errorf("error scanning row: %s", err)
		}
		//t.Logf("%d=%q", num, str)
	}
}

// TestClob
func TestClob(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
}

func TestPrepared(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	stmt, err := conn.Prepare("SELECT ? FROM DUAL")
	if err != nil {
		t.Errorf("error preparing query: %v", err)
		t.FailNow()
	}
	rows, err := stmt.Query("a")
	if err != nil {
		t.Errorf("error executing query: %s", err)
		t.FailNow()
	}
	defer rows.Close()
}

func TestDDL(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	conn.Exec("DROP TABLE TST_ddl")
	defer conn.Exec("DROP TABLE TST_ddl")
	_, err := conn.Exec("CREATE TABLE TST_ddl (key VARCHAR2(3), value VARCHAR2(1000))")
	if err != nil {
		t.Errorf("error creating table TST_ddl: %v", err)
	}
}

var testDB *sql.DB

func getConnection(t *testing.T) *sql.DB {
	var err error
	if testDB != nil && testDB.Ping() == nil {
		return testDB
	}
	flag.Parse()
	if testDB, err = sql.Open("gocilib", *fDsn); err != nil {
		t.Fatalf("error connecting to %q: %s", *fDsn, err)
	}
	return testDB
}
