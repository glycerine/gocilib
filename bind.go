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

/*
#cgo LDFLAGS: -locilib -lclntsh
#include <ocilib.h>
#include <oci.h>

extern unsigned int OCI_NUM_NUMBER;
const int sof_OCI_DateP = sizeof(OCI_Date*);
const int sof_OCI_IntervalP = sizeof(OCI_Interval*);

extern boolean OCI_BindNumber(OCI_Statement *stmt, const mtext *name, OCINumber *data);
extern boolean OCI_BindArrayOfNumbers(OCI_Statement *stmt, const mtext *name, OCINumber *data, unsigned int nbelem);
extern boolean OCI_BindArrayOfNumbers2(OCI_Statement *stmt, const mtext *name, OCINumber **data, unsigned int nbelem);
extern boolean NumberFromDouble(OCIError *err, OCINumber *dst, double src);
extern boolean NumberToText(OCIError *err, char *dst, ub4 *dst_size, OCINumber *src);
*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/tgulacsi/gocilib/sqlhlp"
	"gopkg.in/inconshreveable/log15.v2"
)

func (stmt *Statement) BindPos(pos int, arg driver.Value) error {
	return stmt.BindName(":"+strconv.Itoa(pos), arg)
}

type lengther interface {
	Len() int
}
type capaciter interface {
	Cap() int
}

func (stmt *Statement) BindName(name string, value driver.Value) error {
	h, nm, ok := stmt.handle, C.CString(name), C.int(C.FALSE)
	Log.Debug("BindName", "name", name,
		"type", log15.Lazy{func() string { return fmt.Sprintf("%T", value) }},
		"value", log15.Lazy{func() string { return fmt.Sprintf("%#v", value) }},
		"length", log15.Lazy{func() int {
			switch x := value.(type) {
			case []byte:
				return len(x)
			case []int:
				return len(x)
			case []sqlhlp.NullFloat64:
				return len(x)
			case string:
				return len(x)
			}
			if x, ok := value.(lengther); ok {
				return x.Len()
			}
			return -1
		}},
		"cap", log15.Lazy{func() int {
			switch x := value.(type) {
			case []byte:
				return cap(x)
			case []int:
				return cap(x)
			case []sqlhlp.NullFloat64:
				return cap(x)
			case string:
				return len(x)
			}
			if x, ok := value.(capaciter); ok {
				return x.Cap()
			}
			return -1
		}},
	)
Outer:
	switch x := value.(type) {
	case int16: // short
		ok = C.OCI_BindShort(h, nm, (*C.short)(unsafe.Pointer(&x)))
	case *int16: // short
		ok = C.OCI_BindShort(h, nm, (*C.short)(unsafe.Pointer(x)))
	case []int16:
		ok = C.OCI_BindArrayOfShorts(h, nm, (*C.short)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint16: // unsigned short
		ok = C.OCI_BindUnsignedShort(h, nm, (*C.ushort)(unsafe.Pointer(&x)))
	case *uint16: // unsigned short
		ok = C.OCI_BindUnsignedShort(h, nm, (*C.ushort)(unsafe.Pointer(x)))
	case []uint16:
		ok = C.OCI_BindArrayOfUnsignedShorts(h, nm, (*C.ushort)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case int32: // int
		ok = C.OCI_BindInt(h, nm, (*C.int)(unsafe.Pointer(&x)))
	case []int32:
		ok = C.OCI_BindArrayOfInts(h, nm, (*C.int)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint32: // int
		ok = C.OCI_BindUnsignedInt(h, nm, (*C.uint)(unsafe.Pointer(&x)))
	case *uint32: // int
		ok = C.OCI_BindUnsignedInt(h, nm, (*C.uint)(unsafe.Pointer(x)))
	case []uint32:
		ok = C.OCI_BindArrayOfUnsignedInts(h, nm, (*C.uint)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case int64:
		ok = C.OCI_BindBigInt(h, nm, (*C.big_int)(unsafe.Pointer(&x)))
	case *int64:
		ok = C.OCI_BindBigInt(h, nm, (*C.big_int)(unsafe.Pointer(x)))
	case []int64:
		ok = C.OCI_BindArrayOfBigInts(h, nm, (*C.big_int)(unsafe.Pointer(&x[0])), C.uint(len(x)))
	case uint64:
		ok = C.OCI_BindUnsignedBigInt(h, nm, (*C.big_uint)(unsafe.Pointer(&x)))
	case *uint64:
		ok = C.OCI_BindUnsignedBigInt(h, nm, (*C.big_uint)(unsafe.Pointer(x)))
	case []uint64:
		ok = C.OCI_BindArrayOfUnsignedBigInts(h, nm, (*C.big_uint)(unsafe.Pointer(&x[0])), C.uint(len(x)))

	case sqlhlp.NullInt64:
		ok = C.OCI_BindBigInt(h, nm, (*C.big_int)(unsafe.Pointer(&x.Int64)))
		if ok == C.TRUE && !x.Valid {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case *sqlhlp.NullInt64:
		ok = C.OCI_BindBigInt(h, nm, (*C.big_int)(unsafe.Pointer(&x.Int64)))
		if ok == C.TRUE && !x.Valid {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}

	case string:
		ok = C.OCI_BindString(h, nm, C.CString(x), C.uint(len(x)))
	case *string:
		ok = C.OCI_BindString(h, nm, C.CString(*x), C.uint(len(*x)))
	case StringVar:
		ok = C.OCI_BindString(h, nm, (*C.dtext)(unsafe.Pointer(&x.data[0])), C.uint(cap(x.data)))
		if ok == C.TRUE && len(x.data) == 0 {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case *StringVar:
		ok = C.OCI_BindString(h, nm, (*C.dtext)(unsafe.Pointer(&x.data[0])), C.uint(cap(x.data)))
		if ok == C.TRUE && len(x.data) == 0 {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case []string:
		m := 0
		for _, s := range x {
			if len(s)+1 > m {
				m = len(s) + 1
			}
		}
		if m == 0 {
			m = 32767
		}
		y := make([]byte, m*len(x))
		for i, s := range x {
			copy(y[i*m:(i+1)*m], []byte(s))
		}
		ok = C.OCI_BindArrayOfStrings(h, nm, (*C.dtext)(unsafe.Pointer(&y[0])), C.uint(m), C.uint(len(x)))
	case []byte:
		ok = C.OCI_BindRaw(h, nm, unsafe.Pointer(&x[0]), C.uint(cap(x)))
	case [][]byte:
		m := 0
		for _, b := range x {
			if len(b) > m {
				m = len(b)
			}
		}
		if m == 0 {
			m = 32767
		}
		y := make([]byte, m*len(x))
		for i, b := range x {
			copy(y[i*m:(i+1)*m], b)
		}
		ok = C.OCI_BindArrayOfRaws(h, nm, unsafe.Pointer(&y[0]), C.uint(m), C.uint(cap(x)))
	case float32:
		ok = C.OCI_BindFloat(h, nm, (*C.float)(&x))
	case *float32:
		ok = C.OCI_BindFloat(h, nm, (*C.float)(x))
	case []float32:
		ok = C.TRUE
		//ok = C.OCI_BindArrayOfDoubles(h, nm, (*C.float)(&x[0]), C.uint(cap(x)))
		num := make([]C.OCINumber, len(x), cap(x))
		for i, f := range x {
			var n OCINumber
			num[i] = *n.SetFloat(float64(f)).COCINumber()
		}
		Log.Info("num2", "x", x, "arr", num)
		if ok == C.TRUE {
			ok = C.OCI_BindArrayOfNumbers(h, nm, &num[0], C.uint(cap(num)))
		}
	case float64:
		ok = C.OCI_BindDouble(h, nm, (*C.double)(&x))
	case *float64:
		ok = C.OCI_BindDouble(h, nm, (*C.double)(x))
	case []float64:
		ok = C.TRUE
		if false {
			ok = C.OCI_BindArrayOfDoubles(h, nm, (*C.double)(&x[0]), C.uint(cap(x)))
		} else {
			num := make([]C.OCINumber, len(x), cap(x))
			n := new(OCINumber)
			for i, f := range x {
				num[i] = *(n.SetFloat(f).COCINumber())
				//Log.Debug("setfloat", "f", f, "OCINumber", n.SetFloat(f).String())
			}
			Log.Info("num2", "x", x, "arr", num)
			if ok == C.TRUE {
				ok = C.OCI_BindArrayOfNumbers(h, nm, &num[0], C.uint(cap(num)))
			}
			bnd := C.OCI_GetBind2(h, nm)
			dat := C.OCI_BindGetData(bnd)
			Log.Info("num3", "count", C.OCI_BindGetDataCount(bnd),
				"&num[0]", &num[0], "bnd", dat,
				"first", new(OCINumber).SetCOCINumber(*(*C.OCINumber)(dat)).String())
		}
		/*
			errHandle := (*C.OCIError)(C.OCI_HandleGetError(C.OCI_StatementGetConnection(stmt.handle)))
			num := make([]C.OCINumber, len(x), cap(x))
			for i, d := range x {
				ok = C.NumberFromDouble(errHandle, &num[i], C.double(d))
				if ok != C.TRUE {
					break
				}
			}
			if ok == C.TRUE {
				ok = C.OCI_BindArrayOfNumbers(h, nm, &num[0], C.uint(cap(num)))
			}
		*/
	case sqlhlp.NullFloat64:
		ok = C.OCI_BindDouble(h, nm, (*C.double)(&x.Float64))
		/*
			var num C.OCINumber
			ok = C.NumberFromDouble(
				(*C.OCIError)(C.OCI_HandleGetError(C.OCI_StatementGetConnection(stmt.handle))),
				&num, C.double(x.Float64))
			if ok == C.TRUE {
				ok = C.OCI_BindNumber(h, nm, &num)
			}
		*/
		if ok == C.TRUE && !x.Valid {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case *sqlhlp.NullFloat64:
		ok = C.OCI_BindDouble(h, nm, (*C.double)(&x.Float64))
		if ok == C.TRUE && !x.Valid {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case []sqlhlp.NullFloat64:
		//errHandle := (*C.OCIError)(C.OCI_HandleGetError(C.OCI_StatementGetConnection(stmt.handle)))
		ok = C.TRUE
		arr := make([]*C.OCINumber, len(x), cap(x))
		for i, d := range x {
			if !d.Valid {
				continue
			}
			var n OCINumber
			arr[i] = n.SetFloat(d.Float64).COCINumber()

			/*
				txt := make([]byte, 64)
				buflen := C.ub4(len(txt))
				ok = C.NumberToText(errHandle, (*C.char)(unsafe.Pointer(&txt[0])), &buflen, arr[i])
				if ok != C.TRUE {
					break
				}
				txt = txt[:int(buflen)]
				Log.Info("[]sqlhlp.NullFloat64", "num", d.Float64, "txt", string(txt))
			*/
		}
		if ok == C.TRUE {
			//ok = C.OCI_BindArrayOfNumbers(h, nm, &arr[0], C.uint(cap(arr)))
			ok = C.OCI_BindArrayOfNumbers2(h, nm, &arr[0], C.uint(cap(arr)))
		}
		if ok == C.TRUE {
			bnd := C.OCI_GetBind2(h, nm)
			dat := C.OCI_BindGetData(bnd)
			str := "null"
			if dat != nil {
				str = new(OCINumber).SetCOCINumber(**((**C.OCINumber)(dat))).String()
			}
			Log.Info("nullFloat3", "count", C.OCI_BindGetDataCount(bnd),
				"&arr[0]", &arr[0], "bnd", dat, "first", str)
			for i := 0; i < cap(arr); i++ {
				if i < len(x) && x[i].Valid {
					continue
				}
				if ok = C.OCI_BindSetNullAtPos(bnd, C.uint(i+1)); ok != C.TRUE {
					break
				}
			}
		}

	case OCINumber:
		ok = C.OCI_BindNumber(h, nm, (*C.OCINumber)(x.COCINumber()))
		if ok == C.TRUE {
			ok = C.OCI_BindSetDirection(C.OCI_GetBind2(h, nm), C.OCI_BDM_IN)
		}
	case *OCINumber:
		ok = C.OCI_BindNumber(h, nm, (*C.OCINumber)(x.COCINumber()))
		if ok == C.TRUE && x == nil {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}
	case []OCINumber:
		arr := make([]C.OCINumber, len(x), cap(x))
		for i, n := range x {
			arr[i] = *n.COCINumber()
		}
		ok = C.OCI_BindArrayOfNumbers(h, nm, &arr[0], C.uint(cap(arr)))
		if ok == C.TRUE {
			bnd := C.OCI_GetBind2(h, nm)
			for i := 0; i < cap(arr); i++ {
				if i < len(x) && x[i].Valid() {
					continue
				}
				if ok = C.OCI_BindSetNullAtPos(bnd, C.uint(i+1)); ok != C.TRUE {
					break
				}
			}
		}

	case time.Time:
		od := C.OCI_DateCreate(C.OCI_StatementGetConnection(stmt.handle))
		y, m, d := x.Date()
		H, M, S := x.Clock()
		if C.OCI_DateSetDateTime(od, C.int(y), C.int(m), C.int(d), C.int(H), C.int(M), C.int(S)) != C.TRUE {
			break
		}
		ok = C.OCI_BindDate(h, nm, od)
	case sqlhlp.NullTime:
		od := C.OCI_DateCreate(C.OCI_StatementGetConnection(stmt.handle))
		y, m, d := x.Date()
		H, M, S := x.Clock()
		if C.OCI_DateSetDateTime(od, C.int(y), C.int(m), C.int(d), C.int(H), C.int(M), C.int(S)) != C.TRUE {
			break
		}
		ok = C.OCI_BindDate(h, nm, od)
		if ok == C.TRUE && x.IsZero() {
			ok = C.OCI_BindSetNull(C.OCI_GetBind2(h, nm))
		}

	case []time.Time:
		od := C.OCI_DateArrayCreate(C.OCI_StatementGetConnection(stmt.handle), C.uint(len(x)))
		sof_OCI_DateP := C.int(C.sof_OCI_DateP)
		for i, t := range x {
			y, m, d := t.Date()
			H, M, S := t.Clock()
			if C.OCI_DateSetDateTime(
				(*C.OCI_Date)(unsafe.Pointer(
					uintptr(unsafe.Pointer(od))+uintptr(sof_OCI_DateP*C.int(i))),
				),
				C.int(y), C.int(m), C.int(d), C.int(H), C.int(M), C.int(S),
			) != C.TRUE {
				break Outer
			}
		}
		ok = C.OCI_BindArrayOfDates(h, nm, od, C.uint(len(x)))
	case time.Duration:
		oi := C.OCI_IntervalCreate(C.OCI_StatementGetConnection(stmt.handle), C.OCI_INTERVAL_DS)
		d, H, M, S, ms := durationAsDays(x)
		if C.OCI_IntervalSetDaySecond(oi, C.int(d), C.int(H), C.int(M), C.int(S), C.int(ms/100)) != C.TRUE {
			break
		}
		ok = C.OCI_BindInterval(h, nm, oi)
	case []time.Duration:
		oi := C.OCI_IntervalArrayCreate(C.OCI_StatementGetConnection(stmt.handle), C.OCI_INTERVAL_DS, C.uint(len(x)))
		sof_OCI_IntervalP := C.int(C.sof_OCI_IntervalP)
		for i, t := range x {
			d, H, M, S, ms := durationAsDays(t)
			if C.OCI_IntervalSetDaySecond(
				(*C.OCI_Interval)(unsafe.Pointer(
					uintptr(unsafe.Pointer(oi))+uintptr(sof_OCI_IntervalP*C.int(i)))),
				C.int(d), C.int(H), C.int(M), C.int(S), C.int(ms/100),
			) != C.TRUE {
				break Outer
			}
		}
		ok = C.OCI_BindArrayOfIntervals(h, nm, oi, C.OCI_INTERVAL_DS, C.uint(len(x)))
	case LOB:
		ok = C.OCI_BindLob(h, nm, x.handle)
	case []LOB:
		if len(x) > 0 {
			lo := make([]*C.OCI_Lob, len(x))
			for i := range x {
				lo[i] = x[i].handle
			}
			ok = C.OCI_BindArrayOfLobs(h, nm, (**C.OCI_Lob)(unsafe.Pointer(&lo[0])), x[0].Type(), C.uint(len(x)))
		}
	case File:
		ok = C.OCI_BindFile(h, nm, x.handle)
	case []File:
		if len(x) > 0 {
			fi := make([]*C.OCI_File, len(x))
			for i := range x {
				fi[i] = x[i].handle
			}
			ok = C.OCI_BindArrayOfFiles(h, nm, (**C.OCI_File)(unsafe.Pointer(&fi[0])), x[0].Type(), C.uint(len(x)))
		}
	case Object:
		ok = C.OCI_BindObject(h, nm, x.handle)
	case []Object:
		if len(x) > 0 {
			ob := make([]*C.OCI_Object, len(x))
			for i := range x {
				ob[i] = x[i].handle
			}
			ok = C.OCI_BindArrayOfObjects(h, nm, (**C.OCI_Object)(unsafe.Pointer(&ob[0])), x[0].Type(), C.uint(len(x)))
		}
	case Coll:
		ok = C.OCI_BindColl(h, nm, x.handle)
	case []Coll:
		if len(x) > 0 {
			co := make([]*C.OCI_Coll, len(x))
			for i := range x {
				co[i] = x[i].handle
			}
			ok = C.OCI_BindArrayOfColls(h, nm, (**C.OCI_Coll)(unsafe.Pointer(&co[0])), x[0].Type(), C.uint(len(x)))
		}
	case Ref:
		ok = C.OCI_BindRef(h, nm, x.handle)
	case []Ref:
		if len(x) > 0 {
			re := make([]*C.OCI_Ref, len(x))
			for i := range x {
				re[i] = x[i].handle
			}
			ok = C.OCI_BindArrayOfRefs(h, nm, (**C.OCI_Ref)(unsafe.Pointer(&re[0])), x[0].Type(), C.uint(len(x)))
		}
	case Statement:
		ok = C.OCI_BindStatement(h, nm, x.handle)
	case Long:
		ok = C.OCI_BindLong(h, nm, x.handle, x.Len())
	default:
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Ptr {
			return stmt.BindName(name, v.Elem().Interface())
		}
		return fmt.Errorf("BindName(%s): unknown type %T", name, value)
	}
	if ok != C.TRUE {
		return fmt.Errorf("BindName(%s): %v", name, getLastErr())
	}

	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		ok = C.OCI_BindSetDirection(C.OCI_GetBind2(h, nm), C.OCI_BDM_IN)
	}
	if ok != C.TRUE {
		return fmt.Errorf("BindName(%s): %v", name, getLastErr())
	}
	return nil
}

func durationAsDays(d time.Duration) (days, hours, minutes, seconds, milliseconds int) {
	ns := d.Nanoseconds()
	days = int(ns / int64(time.Hour) / 24)
	ns -= int64(days) * int64(time.Hour) * 240
	hours = int(ns / int64(time.Hour))
	ns -= int64(hours) * int64(time.Hour)
	minutes = int(ns / int64(time.Minute))
	ns -= int64(minutes) * int64(time.Minute)
	seconds = int(ns / int64(time.Second))
	ns -= int64(seconds) * int64(time.Second)
	milliseconds = int(ns / int64(time.Millisecond))
	return
}

func getBindInto(dst driver.Value, bnd *C.OCI_Bind) (val driver.Value, err error) {
	typ, sub := C.OCI_BindGetType(bnd), C.uint(0)
	data := C.OCI_BindGetData(bnd)
	switch typ {
	case C.OCI_CDT_NUMERIC,
		C.OCI_CDT_LOB,
		C.OCI_CDT_FILE,
		C.OCI_CDT_TIMESTAMP,
		C.OCI_CDT_LONG,
		C.OCI_CDT_INTERVAL:
		sub = C.OCI_BindGetSubtype(bnd)
	}

	isNull := data == nil || C.OCI_BindIsNull(bnd) == C.TRUE
	Log.Debug("getBindInto", "bind", bnd,
		"dir", C.OCI_BindGetDirection(bnd),
		"bind", log15.Lazy{func() string { return fmt.Sprintf("%T", dst) }},
		"typ", typ, "sub", sub, "isNull?", isNull,
		"dst", log15.Lazy{func() string { return fmt.Sprintf("%#v", dst) }},
	)

	defer func() {
		Log.Debug("getBindInto", "res", LazyPrint(val), "error", err)
	}()

	switch typ {
	case C.OCI_CDT_NUMERIC:
		var (
			i int64
			u uint64
			f float64
			n OCINumber
		)
		if !isNull {
			switch sub {
			case C.OCI_NUM_SHORT:
				i = int64(int16(*(*C.short)(data)))
				u = uint64(i)
				f = float64(i)
				n.SetInt(i)
			case C.OCI_NUM_INT:
				i = int64(int32(*(*C.int)(data)))
				u = uint64(i)
				f = float64(i)
				n.SetInt(i)
			case C.OCI_NUM_BIGINT:
				i = int64(*(*C.long)(data))
				u = uint64(i)
				f = float64(i)
				n.SetInt(i)
			case C.OCI_NUM_USHORT:
				u = uint64(*(*C.ushort)(data))
				i = int64(u)
				f = float64(u)
				n.SetInt(int64(u))
			case C.OCI_NUM_UINT:
				u = uint64(*(*C.uint)(data))
				i = int64(u)
				f = float64(u)
				n.SetInt(int64(u))
			case C.OCI_NUM_BIGUINT:
				u = uint64(*(*C.ulong)(data))
				i = int64(u)
				f = float64(u)
				n.SetInt(int64(u))
			case C.OCI_NUM_FLOAT:
				f = float64(*(*C.float)(data))
				i = int64(f)
				u = uint64(f)
				n.SetFloat(f)
			case C.OCI_NUM_DOUBLE:
				f = float64(*(*C.double)(data))
				i = int64(f)
				u = uint64(f)
				n.SetFloat(f)
			case C.OCI_NUM_NUMBER:
				n.SetCOCINumberP((*C.OCINumber)(data))
				dec := n.Dec(nil)
				unscaled := dec.UnscaledBig()
				scale := int32(dec.Scale())
				scaled := unscaled
				rat := big.NewRat(1, 1)
				rat.SetInt(unscaled)
				if scale > 0 {
					mul := int64(1)
					for j := int32(0); j < scale; j++ {
						mul *= 10
					}
					mI := big.NewInt(mul)
					scaled = scaled.Mul(unscaled, mI)
					rat.SetInt(scaled)
				} else {
					mul := int64(1)
					for j := int32(0); j > scale; j-- {
						mul *= 10
					}
					mI := big.NewInt(mul)
					scaled.Div(scaled, mI)
					rat.SetFrac(unscaled, mI)
				}
				i = scaled.Int64()
				f, _ = rat.Float64()
				u = uint64(i)
			}
		}

		Log.Debug("data", "int", i, "uint", u, "float", f, "OCINumber", LazyPrint(n.String()))

		switch x := dst.(type) {
		case int16:
			return int16(i), nil
		case *int16:
			*x = int16(i)
		case uint16:
			return uint16(i), nil
		case *uint16:
			*x = uint16(i)
		case int32:
			return int32(i), nil
		case *int32:
			*x = int32(i)
		case int:
			return int(i), nil
		case *int:
			*x = int(i)
		case uint32:
			return uint32(i), nil
		case *uint32:
			*x = uint32(i)
		case uint:
			return uint(i), nil
		case *uint:
			*x = uint(i)
		case int64:
			return i, nil
		case *int64:
			*x = i
		case uint64:
			return uint64(i), nil
		case *uint64:
			*x = uint64(i)

		case sqlhlp.NullInt64:
			if C.OCI_BindIsNull(bnd) == C.TRUE {
				return sqlhlp.NullInt64{Valid: false}, nil
			}
			return sqlhlp.NullInt64{Valid: true, Int64: i}, nil
		case *sqlhlp.NullInt64:
			if isNull {
				x.Valid = false
			} else {
				x.Valid = true
				x.Int64 = i
			}

		case float32:
			return float32(f), nil
		case *float32:
			*x = float32(f)
		case float64:
			return f, nil
		case *float64:
			*x = f

		case sqlhlp.NullFloat64:
			if isNull {
				return sqlhlp.NullFloat64{Valid: false}, nil
			}
			return sqlhlp.NullFloat64{Valid: true, Float64: f}, nil
		case *sqlhlp.NullFloat64:
			if isNull {
				x.Valid = false
			} else {
				x.Valid = true
				x.Float64 = f
			}

		case OCINumber:
			if isNull {
				return OCINumber{}, nil
			}
			return n, nil
		case *OCINumber:
			if isNull {
				x.SetString("")
			} else {
				x.Set(&n)
			}
			return x, nil

		default:
			return dst, fmt.Errorf("bindInto: int is needed, not %T", dst)
		}
		return dst, nil

	case C.OCI_CDT_DATETIME:
		var y, m, d, H, M, S C.int
		if !isNull {
			if C.OCI_DateGetDateTime((*C.OCI_Date)(data), &y, &m, &d, &H, &M, &S) != C.TRUE {
				return dst, fmt.Errorf("error reading date: %v", getLastErr())
			}
		}
		switch x := dst.(type) {
		case time.Time:
			if isNull {
				return time.Time{}, nil
			}
			return time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), 0, time.Local), nil
		case *time.Time:
			if isNull {
				*x = time.Time{}
				return x, nil
			}
			*x = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), 0, time.Local)
			return x, nil
		case sqlhlp.NullTime:
			if isNull {
				return sqlhlp.NullTime{Valid: false}, nil
			}
			return sqlhlp.NullTime{
				Valid: true,
				Time:  time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), 0, time.Local),
			}, nil
		case *sqlhlp.NullTime:
			x.Valid = false
			if isNull {
				return x, nil
			}
			x.Valid = true
			x.Time = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), 0, time.Local)
			return x, nil
		default:
			return dst, fmt.Errorf("time needs time.Time, not %T!", dst)
		}

	case C.OCI_CDT_TIMESTAMP:
		var y, m, d, H, M, S, f, offH, offM C.int
		var tz *time.Location
		if !isNull {
			if C.OCI_TimestampGetDateTime((*C.OCI_Timestamp)(data), &y, &m, &d, &H, &M, &S, &f) != C.TRUE {
				return dst, fmt.Errorf("error reading timestamp: %v", getLastErr())
			}
			if C.OCI_TimestampGetTimeZoneOffset((*C.OCI_Timestamp)(data), &offH, &offM) != C.TRUE {
				return dst, fmt.Errorf("error reading tz offset: %v", getLastErr())
			}
			tz = time.FixedZone(fmt.Sprintf("%+02d:%02d", offH, offM), int(offH*3600+offM*60))
		}
		switch x := dst.(type) {
		case time.Time:
			if isNull {
				return time.Time{}, nil
			}
			return time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), int(f), tz), nil
		case sqlhlp.NullTime:
			if isNull {
				return sqlhlp.NullTime{Valid: false}, nil
			}
			return sqlhlp.NullTime{
				Valid: true,
				Time:  time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), int(f), tz),
			}, nil
		case *time.Time:
			if isNull {
				*x = time.Time{}
				return x, nil
			}
			*x = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), int(f), tz)
			return x, nil
		case *sqlhlp.NullTime:
			x.Valid = false
			if isNull {
				return x, nil
			}
			x.Valid = true
			x.Time = time.Date(int(y), time.Month(m), int(d), int(H), int(M), int(S), int(f), tz)
			return x, nil

		default:
			return dst, fmt.Errorf("time needs time.Time or sqlhlp.NullTime, not %T!", dst)
		}

	case C.OCI_CDT_TEXT:
		switch x := dst.(type) {
		case string:
			return C.GoString((*C.char)(data)), nil
		case *string:
			*x = C.GoString((*C.char)(data))
			return dst, nil
		case []byte:
			return byteString(data), nil
		case StringVar:
			return StringVar{data: byteString(data)}, nil
		case *StringVar:
			x.data = byteString(data)
			return dst, nil
		default:
			return dst, fmt.Errorf("text needs string, not %T!", dst)
		}
	}
	return dst, nil
}

func byteString(data unsafe.Pointer) []byte {
	if data == nil {
		return nil
	}
	b := (*[32767]byte)(data)[0:32767:32767]
	for i, c := range b {
		if c == 0 {
			b = b[:i+1]
			break
		}
	}
	return b[:len(b):len(b)]
}

func LazyPrint(v interface{}) log15.Lazy {
	return log15.Lazy{func() string { return fmt.Sprintf("%#v", v) }}
}
