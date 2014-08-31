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

package sqlhlp

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"time"
)

type NullBool sql.NullBool

func (b NullBool) MarshalJSON() ([]byte, error) {
	if !b.Valid {
		return []byte("false"), nil
	}
	return json.Marshal(b.Bool)
}
func (b *NullBool) UnmarshalJSON(data []byte) error {
	b.Valid = false
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &b.Bool); err != nil {
		return err
	}
	b.Valid = true
	return nil
}

type NullInt64 sql.NullInt64

func (i NullInt64) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return nil, nil
	}
	return json.Marshal(i.Int64)
}

func (i *NullInt64) UnmarshalJSON(data []byte) error {
	i.Valid = false
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &i.Int64); err != nil {
		return err
	}
	i.Valid = true
	return nil
}

type NullFloat64 sql.NullFloat64

func (f NullFloat64) MarshalJSON() ([]byte, error) {
	if !f.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(f.Float64)
}

func (f *NullFloat64) UnmarshalJSON(data []byte) error {
	f.Valid = false
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &f.Float64); err != nil {
		return err
	}
	f.Valid = true
	return nil
}

type NullTime struct {
	Valid bool
	time.Time
}

func (t NullTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

func (t *NullTime) UnmarshalJSON(data []byte) error {
	t.Valid = false
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	t.Valid = true
	return t.Time.UnmarshalJSON(data)
}
