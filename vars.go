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

type StringVar struct {
	data []byte
}

// NewStringVar returns a new StringVar, filled with the given text.
// It's size will be given size, or the text's length if the size is
// less than the text's length.
func NewStringVar(text string, size int) StringVar {
	if len(text) > 32767 {
		text = text[:32767]
	}
	if size < len(text) {
		size = len(text)
	}
	b := make([]byte, len(text)+1, size+1)
	copy(b, []byte(text))
	b[len(text)] = 0 // trailing zero
	return StringVar{data: b}
}

// String returns the string representation of the underlying data.
func (s StringVar) String() string {
	return string(s.data)
}
