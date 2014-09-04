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
	"testing"

	"github.com/tgulacsi/gocilib/sqlhlp"
	"gopkg.in/inconshreveable/log15.v2"
)

func TestInBindIntArray(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)
	conn := getConnection(t)
	st, err := conn.NewStatement()
	if err != nil {
		t.Errorf("new statement: %v", err)
		return
	}
	defer st.Close()

	ret := NewStringVar("", 10)
	if err := st.BindExecute(`DECLARE
  TYPE num_tab_typ IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  tab num_tab_typ := :1;
BEGIN
  :2 := ''||tab.COUNT;
END;`,
		[]driver.Value{[]int{1, 2}, ret}, nil,
	); err != nil {
		t.Errorf("err: %v", err)
	}
	if ret.String() != "2" {
		t.Errorf("awaited 2, got %q", ret.String())
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

	ret := NewStringVar("", 10)
	if err := st.BindExecute(`DECLARE
  TYPE num_tab_typ IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  tab num_tab_typ := :1;
BEGIN
  :2 := ''||tab.COUNT;
END;`,
		[]driver.Value{[]sqlhlp.NullFloat64{
			sqlhlp.NullFloat64{Valid: true, Float64: 1},
			sqlhlp.NullFloat64{Valid: true, Float64: 2},
		}, ret}, nil,
	); err != nil {
		t.Errorf("err: %v", err)
	}
	if ret.String() != "2" {
		t.Errorf("awaited 2, got %q", ret.String())
	}
}
