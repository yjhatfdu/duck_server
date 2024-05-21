package main

import (
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"strconv"
	"strings"
	"time"
)

type pgType struct {
	Oid    int32
	Name   string
	Typlen int16
}

var pgTypes = []pgType{
	{16, "bool", 0},
	{17, "bytea", 0},
	{18, "char", 0},
	{20, "int8", 0},
	{21, "int4", 0},
	{700, "float4", 0},
	{701, "float8", 0},
	{25, "text", 0},
	{1700, "numeric", 0},
	{1114, "timestamp", 0},
}

var oidTypeMap = map[int32]pgType{}
var typeOidMap = map[string]int32{}

func init() {
	for _, t := range pgTypes {
		oidTypeMap[t.Oid] = t
		typeOidMap[t.Name] = t.Oid
	}
}

func pgTypeFromOid(oid int32) pgType {
	return oidTypeMap[oid]
}

func pgOidFromType(name string) int32 {
	return typeOidMap[name]
}

type pgValue struct {
	typ pgType
	val []byte
}

func toPgValue(v any) (pgValue, error) {
	switch v := v.(type) {
	case bool:
		var b []byte
		if v {
			b = cstr("t")
		} else {
			b = cstr("f")
		}
		return pgValue{pgTypeFromOid(16), b}, nil
	case int8:
		s := strconv.FormatInt(int64(v), 10)
		b := []byte(s)
		return pgValue{pgTypeFromOid(18), b}, nil
	case int16:
		s := strconv.FormatInt(int64(v), 10)
		b := []byte(s)
		return pgValue{pgTypeFromOid(21), b}, nil
	case int32:
		s := strconv.FormatInt(int64(v), 10)
		b := []byte(s)
		return pgValue{pgTypeFromOid(21), b}, nil
	case int64:
		s := strconv.FormatInt(v, 10)
		b := []byte(s)
		return pgValue{pgTypeFromOid(20), b}, nil
	case float32:
		s := strconv.FormatFloat(float64(v), 'f', -1, 32)
		b := []byte(s)
		return pgValue{pgTypeFromOid(701), b}, nil
	case float64:
		s := strconv.FormatFloat(v, 'f', -1, 64)
		b := []byte(s)
		return pgValue{pgTypeFromOid(701), b}, nil
	case string:
		b := []byte(v)
		return pgValue{pgTypeFromOid(25), b}, nil
	case nil:
		return pgValue{pgTypeFromOid(25), nil}, nil
	case duckdb.Decimal:
		f := v.Float64()
		s := strconv.FormatFloat(f, 'f', -1, 64)
		b := []byte(s)
		return pgValue{pgTypeFromOid(1700), b}, nil
	case time.Time:
		s := v.Format("2006-01-02 15:04:05.999999")
		b := []byte(s)
		return pgValue{pgTypeFromOid(25), b}, nil
	case []any:
		var res []string
		for _, e := range v {
			pv, err := toPgValue(e)
			if err != nil {
				return pgValue{}, err
			}
			res = append(res, string(pv.val))
		}
		b := []byte("{" + strings.Join(res, ",") + "}")
		return pgValue{pgTypeFromOid(25), b}, nil

	default:
		return pgValue{}, fmt.Errorf("unsupported type %T", v)
	}
}
