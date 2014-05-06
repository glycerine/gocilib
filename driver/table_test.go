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
	"fmt"
	"math/big"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	conn.Exec("DROP TABLE tst_goracle")
	if _, err := conn.Exec(`CREATE TABLE tst_goracle (
			F_int NUMBER(10,0), F_bigint NUMBER(20),
			F_real NUMBER(6,3), F_bigreal NUMBER(20,10),
			F_text VARCHAR2(1000), F_date DATE
		)`); err != nil {
		t.Skipf("Skipping table test, as cannot create tst_goracle: %v", err)
		return
	}
	defer conn.Exec("DROP TABLE tst_goracle")
	tx, err := conn.Begin()
	if err != nil {
		t.Errorf("cannot start transaction: %v", err)
		t.FailNow()
	}
	defer tx.Rollback()

	insert(t, tx, 1234567890, "1234567890123456", 123.456,
		"123456789.123456789", "int64", time.Now())

	insert(t, tx, 2, "22345678901234567890", 223.456,
		"223456789.123456789", "big.Int", time.Now())
}

func insert(t *testing.T, conn *sql.Tx,
	small int, bigint string,
	notint float64, bigreal string,
	text string, date time.Time,
) bool {
	qry := fmt.Sprintf(`INSERT INTO tst_goracle
			(F_int, F_bigint, F_real, F_bigreal, F_text, F_date)
			VALUES (%d, %s, %3.3f, %s, '%s', TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))
			`, small, bigint, notint, bigreal, text, date.Format("2006-01-02 15:04:05"))
	if _, err := conn.Exec(qry); err != nil {
		t.Errorf("cannot insert into tst_goracle (%q): %v", qry, err)
		return false
	}
	row := conn.QueryRow("SELECT * FROM tst_goracle WHERE F_int = :1", small)
	var (
		smallO           int
		bigintI, bigintO big.Int
		//bigintS            string
		notintO            float64
		bigrealI, bigrealO big.Rat
		bigrealS           string
		textO              string
		dateO              time.Time
	)
	if err := row.Scan(&smallO, &bigintO, &notintO, &bigrealS, &textO, &dateO); err != nil {
		t.Errorf("error scanning row: %v", err)
		return false
	}
	t.Logf("row: small=%d big=%d notint=%f bigreal=%f text=%q date=%s",
		smallO, bigintO, notintO, bigrealS, textO, dateO)

	if smallO != small {
		t.Errorf("small mismatch: got %d, awaited %d.", smallO, small)
	}
	(&bigintI).SetString(bigint, 10)
	//(&bigintO).SetString(bigintS, 10)
	if (&bigintO).Cmp(&bigintI) != 0 {
		t.Errorf("bigint mismatch: got %s, awaited %s.", &bigintO, &bigintI)
	}
	if notintO != notint {
		t.Errorf("noting mismatch: got %d, awaited %d.", notintO, notint)
	}
	(&bigrealI).SetString(bigreal)
	(&bigrealO).SetString(bigrealS)
	if (&bigrealO).Cmp(&bigrealI) != 0 {
		t.Errorf("bigreal mismatch: got %s, awaited %s.", (&bigrealO), (&bigrealI))
	}
	if textO != text {
		t.Errorf("text mismatch: got %q, awaited %q.", textO, text)
	}
	if !dateO.Equal(date.Round(time.Second)) {
		t.Errorf("date mismatch: got %s, awaited %s.", dateO, date)
	}

	return true
}
