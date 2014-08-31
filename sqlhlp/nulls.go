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
	"fmt"
	"time"
)

// NullBool is sql.NullBool with JSON Marshaling.
type NullBool sql.NullBool

// String returns the string representation of the bool.
func (b NullBool) String() string {
	if !b.Valid {
		return ""
	} else if b.Bool {
		return "true"
	}
	return "false"
}

// MarshalJSON marshals either null, or the bool.
func (b NullBool) MarshalJSON() ([]byte, error) {
	if !b.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(b.Bool)
}

// UnmarshalJSON unmarshals the bool, with "null" and "" as nil.
func (b *NullBool) UnmarshalJSON(data []byte) error {
	b.Valid = false
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	if err := json.Unmarshal(data, &b.Bool); err != nil {
		return err
	}
	b.Valid = true
	return nil
}

// NullInt64 is sql.NullBool with JSON Marshaling.
type NullInt64 sql.NullInt64

// String returns the string representation of the int.
func (i NullInt64) String() string {
	if !i.Valid {
		return ""
	}
	return fmt.Sprintf("%d", i.Int64)
}

// MarshalJSON marshals either null, or the int.
func (i NullInt64) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(i.Int64)
}

// UnmarshalJSON unmarshals the int, with "null" and "" as empty.
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

// NullFloat64 is sql.NullBool with JSON Marshaling.
type NullFloat64 sql.NullFloat64

// String returns the string representation of the float.
func (f NullFloat64) String() string {
	if !f.Valid {
		return ""
	}
	return fmt.Sprintf("%f", f.Float64)
}

// MarshalJSON marshals either null, or the float.
func (f NullFloat64) MarshalJSON() ([]byte, error) {
	if !f.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(f.Float64)
}

// UnmarshalJSON unmarshals the time, with "null" and "" as empty.
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

// NullTime is sql.NullBool with JSON Marshaling.
type NullTime struct {
	Valid bool
	time.Time
}

// String returns the string representation of the time.
func (t NullTime) String() string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

// MarshalJSON marshals either null, or the time.
func (t NullTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	return t.Time.MarshalJSON()
}

// UnmarshalJSON unmarshals the time, with "null" and "" as empty.
func (t *NullTime) UnmarshalJSON(data []byte) error {
	t.Valid = false
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	t.Valid = true
	return t.Time.UnmarshalJSON(data)
}
