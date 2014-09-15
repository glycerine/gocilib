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

package gocilib

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/tgulacsi/gocilib/sqlhlp"
	"gopkg.in/inconshreveable/log15.v2"
)

func init() {
	Log.SetHandler(log15.StderrHandler)
}

func TestInBindIntArray(t *testing.T) {
	conn := getConnection(t)
	st, err := conn.NewStatement()
	if err != nil {
		t.Errorf("new statement: %v", err)
		return
	}
	defer st.Close()

	ret := NewStringVar("", 10)
	if err := st.BindExecute(`DECLARE
  TYPE str_tab_typ IS TABLE OF VARCHAR2(100) INDEX BY BINARY_INTEGER;
  tab str_tab_typ := :1;
BEGIN
  :2 := ''||tab.COUNT;
END;`,
		[]driver.Value{[]string{"1", "2"}, ret}, nil,
	); err != nil {
		t.Errorf("err: %v", err)
	}
	if ret.String() != "2" {
		t.Errorf("awaited 2, got %q", ret.String())
	}
}
func TestInBindFloat(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)
	conn := getConnection(t)

	if err := conn.SetServerOutput(1000000); err != nil {
		t.Fatalf("cannot set server output: %v", err)
	}
	var n OCINumber
	var lines []string
	st, err := conn.NewStatement()
	if err != nil {
		t.Errorf("new statement: %v", err)
		return
	}
	defer st.Close()
	for i, inp := range []driver.Value{
		*n.SetString("3.14"),
		sqlhlp.NullFloat64{Valid: true, Float64: 3.14},
		float32(3.14),
		float64(3.14),
	} {
		ret := NewStringVar("", 1000)
		params := []driver.Value{inp, ret}
		err = st.BindExecute(`DECLARE
	num NUMBER := :1; dmp VARCHAR2(1000);
BEGIN
  SELECT DUMP(num) INTO dmp FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('num='||num||' = '||dmp);
  :2 := TO_CHAR(num);
END;`,
			params, nil,
		)
		t.Logf("%d. params=%#v err=%v", i, params, err)
		if err != nil {
			t.Errorf("%d. err: %v", i, err)
			break
		}
		t.Logf("%d. serveroutput lines: %s", i, conn.GetServerOutput(lines[:0], -1))
		if !strings.HasPrefix(ret.String(), "3.140") {
			t.Errorf("%d. awaited 2, got %q", i, ret.String())
			break
		}
	}
}

func TestInBindFloatArray(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)
	conn := getConnection(t)
	st, err := conn.NewStatement()
	if err != nil {
		t.Errorf("new statement: %v", err)
		return
	}
	defer st.Close()

	const await = "1=1;2=2;3=3;"
	for i, inp := range []driver.Value{
		[]int16{1, 2, 3},
		[]int32{1, 2, 3},
		[]int64{1, 2, 3},
		[]float32{1, 2, 3}, //3
		[]float64{1, 2, 3},
		[]sqlhlp.NullFloat64{{Valid: true, Float64: 1}, {Valid: true, Float64: 2}, {Valid: true, Float64: 3}},
		[]sqlhlp.NullFloat64{{Valid: true, Float64: 1}},
	} {
		ret := NewStringVar("", 1000)
		if err := st.BindExecute(`DECLARE
  TYPE num_tab_typ IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  tab num_tab_typ := :1;
  v_idx PLS_INTEGER;
  v_txt VARCHAR2(1000);
BEGIN
  v_idx := tab.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_txt := v_txt||v_idx||'='||tab(v_idx)||';';
    v_idx := tab.NEXT(v_idx);
  END LOOP;
  :2 := v_txt;
END;`,
			[]driver.Value{inp, ret}, nil,
		); err != nil {
			t.Errorf("%d. err: %v", i, err)
			continue
		}
		if ret.String() != await {
			t.Errorf("%d. awaited "+await+", got %q", i, ret.String())
			continue
		}
	}
}
