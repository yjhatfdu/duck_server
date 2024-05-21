package main

import (
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

var duck2pgTypeMap = map[string]string{
	"BOOLEAN":   "bool",
	"VARCHAR":   "text",
	"INTEGER":   "int4",
	"BIGINT":    "int8",
	"DOUBLE":    "float8",
	"TIMESTAMP": "timestamp",
	"DECIMAL":   "numeric",
	"DATE":      "date",
	"VARCHAR[]": "text",
}

func duck2pgType(s string) string {
	v, ok := duck2pgTypeMap[s]
	if ok {
		return v
	} else {
		logrus.Panicf("duck2pgType unsupported type %s", s)
	}
	return ""
}

type converter func(in string) (driver.Value, error)

var converters = map[string]converter{
	"INTEGER": func(in string) (driver.Value, error) {
		d, err := strconv.ParseInt(in, 10, 32)
		return int32(d), err
	},
	"VARCHAR": func(in string) (driver.Value, error) {
		return in, nil
	},
	"BIGINT": func(in string) (driver.Value, error) {
		d, err := strconv.ParseInt(in, 10, 64)
		return d, err
	},
	"BOOLEAN": func(in string) (driver.Value, error) {
		d, err := strconv.ParseBool(in)
		return d, err
	},
	"DOUBLE": func(in string) (driver.Value, error) {
		d, err := strconv.ParseFloat(in, 64)
		return d, err
	},
	"BIT": func(in string) (driver.Value, error) {
		d, err := strconv.ParseInt(in, 10, 64)
		return d, err
	},
}

func getDuckDBConverter(typ string) converter {
	return converters[typ]
}

func duckDecimalToString(value duckdb.Decimal) string {
	str := value.Value.String()
	if value.Scale == 0 {
		return str
	}
	if len(str) <= int(value.Scale) {
		zeroCount := int(value.Scale) - len(str)
		return "0." + strings.Repeat("0", zeroCount) + str
	}
	return str[:len(str)-int(value.Scale)] + "." + str[len(str)-int(value.Scale):]
}

func duckValueToString(value any) string {
	switch v := value.(type) {
	case nil:
		return "\\N"
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case string:
		return v
	case bool:
		if v {
			return "1"
		} else {
			return "0"
		}
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	case duckdb.Decimal:
		return duckDecimalToString(v)
	case []any:
		var res []string
		for _, e := range v {
			res = append(res, duckValueToString(e))
		}
		return "{" + strings.Join(res, ",") + "}"
	default:
		logrus.Infof("unsupported type %T", v)
		return fmt.Sprintf("%v", v)
	}
}
