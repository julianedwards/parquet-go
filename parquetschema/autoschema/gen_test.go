package autoschema

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestGenerateSchema(t *testing.T) {
	tests := map[string]struct {
		Input          interface{}
		ExpectErr      bool
		ExpectedOutput string
	}{
		"simple types": {
			Input: struct {
				Foo  string
				Bar  int
				Baz  uint
				Quux float64
				Bla  int64
				Abc  uint64
				Def  float32
				Ghi  int32
				Jkl  uint32
				Mno  int16
				Pqr  uint16
				Rst  int8
				Uvw  uint8
				Xyz  bool
			}{},
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required binary foo (STRING);\n  required int64 bar (INT(64, true));\n  required int32 baz (INT(32, false));\n  required double quux;\n  required int64 bla (INT(64, true));\n  required int64 abc (INT(64, false));\n  required float def;\n  required int32 ghi (INT(32, true));\n  required int32 jkl (INT(32, false));\n  required int32 mno (INT(16, true));\n  required int32 pqr (INT(16, false));\n  required int32 rst (INT(8, true));\n  required int32 uvw (INT(8, false));\n  required boolean xyz;\n}\n",
		},
		"optional type": {
			Input: struct {
				Foo *int
			}{},
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional int64 foo (INT(64, true));\n}\n",
		},
		"struct pointer": {
			Input: (*struct {
				Foo int
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required int64 foo (INT(64, true));\n}\n",
		},
		"structs within struct": {
			Input: (*struct {
				Foo *struct {
					Bar int32
				}
				Baz struct {
					Quux int64
				}
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional group foo {\n    required int32 bar (INT(32, true));\n  }\n  required group baz {\n    required int64 quux (INT(64, true));\n  }\n}\n",
		},
		"slices": {
			Input: (*struct {
				Foo []int
				Bar []*int
				Baz []struct {
					Quux int
				}
				Bla []*struct {
					Fasel *int
				}
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required group foo (LIST) {\n    repeated group list {\n      required int64 element (INT(64, true));\n    }\n  }\n  optional group bar (LIST) {\n    repeated group list {\n      required int64 element (INT(64, true));\n    }\n  }\n  required group baz (LIST) {\n    repeated group list {\n      required group element {\n        required int64 quux (INT(64, true));\n      }\n    }\n  }\n  optional group bla (LIST) {\n    repeated group list {\n      required group element {\n        optional int64 fasel (INT(64, true));\n      }\n    }\n  }\n}\n",
		},
		"arrays": {
			Input: (*struct {
				Foo [1]int
				Bar [10]*int
				Baz [5]struct {
					Quux int
				}
				Bla [23]*struct {
					Fasel *int
				}
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required group foo (LIST) {\n    repeated group list {\n      required int64 element (INT(64, true));\n    }\n  }\n  optional group bar (LIST) {\n    repeated group list {\n      required int64 element (INT(64, true));\n    }\n  }\n  required group baz (LIST) {\n    repeated group list {\n      required group element {\n        required int64 quux (INT(64, true));\n      }\n    }\n  }\n  optional group bla (LIST) {\n    repeated group list {\n      required group element {\n        optional int64 fasel (INT(64, true));\n      }\n    }\n  }\n}\n",
		},
		"byte slices": {
			Input: (*struct {
				Foo []byte
				Bar *[]byte
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required binary foo;\n  optional binary bar;\n}\n",
		},
		"struct tags": {
			Input: (*struct {
				Int64                 int64               `parquet:"name=int_64"`
				StringByteSlice       []byte              `parquet:"name=string_byte_slice, logicaltype=STRING"`
				StringString          *string             `parquet:"name=string_string"`
				EnumByteSlice         []byte              `parquet:"name=enum_byte_slice, logicaltype=ENUM"`
				EnumString            string              `parquet:"name=enum_string, logicaltype=ENUM"`
				DecimalInt32          int32               `parquet:"name=decimal_int_32, logicaltype=DECIMAL, scale=2, precision=3"`
				DecimalInt            int                 `parquet:"name=decimal_int, logicaltype=DECIMAL, scale=5, precision=15"`
				DecimalInt64          int64               `parquet:"name=decimal_int_64, logicaltype=DECIMAL, scale=5, precision=15"`
				DecimalByteSlice      []byte              `parquet:"name=decimal_byte_slice, logicaltype=DECIMAL, scale=10, precision=100"`
				DecimalByteArray      [8]byte             `parquet:"name=decimal_byte_array, logicaltype=DECIMAL, scale=10, precision=8"`
				DateInt32             int32               `parquet:"name=date_int_32, logicaltype=DATE"`
				DateInt64             int32               `parquet:"name=date_int_64, logicaltype=DATE"` // TODO: This may be problematic, though we are downcasting so maybe not.
				DateTime              *time.Time          `parquet:"name=date_time, logicaltype=DATE"`
				TimeMillisInt32       int32               `parquet:"name=time_millis_int_32, logicaltype=TIME, timeunit=MILLIS, isadjustedtoutc=false"`
				TimeMillisInt         int32               `parquet:"name=time_millis_int, logicaltype=TIME, timeunit=MILLIS, isadjustedtoutc=false"`
				TimeMillisInt64       int64               `parquet:"name=time_millis_int_64, logicaltype=TIME, timeunit=MILLIS, isadjustedtoutc=true"` // TODO: This may be problematic, though we are downcasting so maybe not.
				TimeMicrosInt64       int64               `parquet:"name=time_micros_int_64, logicaltype=TIME, timeunit=MICROS, isadjustedtoutc=true"`
				TimeNanosInt64        int64               `parquet:"name=time_nanos_int_64, logicaltype=TIME, timeunit=NANOS, isadjustedtoutc=true"`
				TimeDefaultInt64      int64               `parquet:"name=time_default_int_64, logicaltype=TIME"`
				TimeMillisTime        time.Time           `parquet:"name=time_millis_time, logicaltype=TIME, timeunit=MILLIS, isadjustedtoutc=false"` // TODO: This may be problematic.
				TimeMicrosTime        time.Time           `parquet:"name=time_micros_time, logicaltype=TIME, timeunit=MICROS, isadjustedtoutc=true"`
				TimeNanosTime         time.Time           `parquet:"name=time_nanos_time, logicaltype=TIME, timeunit=NANOS, isadjustedtoutc=true"`
				TimeDefaultTime       time.Time           `parquet:"name=time_default_time, logicaltype=TIME"`
				TimestampMillisInt64  int64               `parquet:"name=ts_millis_int_64, logicaltype=TIMESTAMP, timeunit=MILLIS, isadjustedtoutc=true"`
				TimestampMicrosInt64  int64               `parquet:"name=ts_micros_int_64, logicaltype=TIMESTAMP, timeunit=MICROS, isadjustedtoutc=true"`
				TimestampNanosInt64   *int64              `parquet:"name=ts_nanos_int_64, logicaltype=TIMESTAMP, timeunit=NANOS, isadjustedtoutc=true"`
				TimestampDefaultInt64 int64               `parquet:"name=ts_default_int_64, logicaltype=TIMESTAMP"`
				TimestampMillisTime   time.Time           `parquet:"name=ts_millis_time, logicaltype=TIMESTAMP, timeunit=MILLIS, isadjustedtoutc=false"`
				TimestampMicrosTime   time.Time           `parquet:"name=ts_micros_time, logicaltype=TIMESTAMP, timeunit=MICROS, isadjustedtoutc=true"`
				TimestampNanosTime    time.Time           `parquet:"name=ts_nanos_time, logicaltype=TIMESTAMP, timeunit=NANOS, isadjustedtoutc=true"`
				TimestampDefaultTime  time.Time           `parquet:"name=ts_default_time, logicaltype=TIMESTAMP"`
				JSONByteSlice         []byte              `parquet:"name=json_byte_slice, logicaltype=JSON"`
				JSONString            string              `parquet:"name=json_string, logicaltype=JSON"`
				BSONByteSlice         []byte              `parquet:"name=bson_byte_slice, logicaltype=BSON"`
				BSONString            string              `parquet:"name=bson_string, logicaltype=BSON"`
				UUIDByteArray         [16]byte            `parquet:"name=uuid_byte_array, logicaltype=UUID"`
				ListInt64             []int64             `parquet:"name=list_int_64"`
				ListDecimalByteSlice  [][]byte            `parquet:"name=list_decimal_byte_slice, element.logicaltype=DECIMAL, element.scale=10, element.precision=100"`
				ListTimeMillisTime    []time.Time         `parquet:"name=list_time_millis_time, element.logicaltype=TIME, element.timeunit=MILLIS, element.isadjustedtoutc=true"`
				MapStringInt64        map[string]int64    `parquet:"name=map_string_int_64"`
				MapDecimalTime        map[int64]time.Time `parquet:"name=map_decimal_time, key.logicaltype=DECIMAL, key.scale=5, key.precision=15, value.logicaltype=TIME, value.timeunit=MILLIS"`
				Struct                struct {
					Int64              *int64      `parquet:"name=int_64"`
					TimeMillisInt32    int32       `parquet:"name=time_millis_int_32, logicaltype=TIME, timeunit=MILLIS, isadjustedtoutc=false"`
					ListTimeMillisTime []time.Time `parquet:"name=list_time_millis_time, element.logicaltype=TIME, element.timeunit=MILLIS, element.isadjustedtoutc=true"`
				} `parquet:"name=struct"`
				ListStruct []struct {
					StringByteSlice []byte `parquet:"name=string_byte_slice, logicaltype=STRING"`
					DateInt32       *int32 `parquet:"name=date_int_32, logicaltype=DATE"`
				} `parquet:"name=list_struct"`
				ListMapDateString []map[int64][]byte     `parquet:"name=list_map_date_string, key.logicaltype=TIME, key.isadjustedtoutc=true, value.logicaltype=STRING"`
				MapStringListTime map[string][]time.Time `parquet:"name=map_string_list_time, element.logicaltype=TIME, element.timeunit=MILLIS, element.isadjustedtoutc=true"`
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required int64 int_64 (INT(64, true));\n  required binary string_byte_slice (STRING);\n  optional binary string_string (STRING);\n  required binary enum_byte_slice (ENUM);\n  required binary enum_string (ENUM);\n  required int32 decimal_int_32 (DECIMAL(3, 2));\n  required int64 decimal_int (DECIMAL(15, 5));\n  required int64 decimal_int_64 (DECIMAL(15, 5));\n  required binary decimal_byte_slice (DECIMAL(100, 10));\n  required fixed_len_byte_array(8) decimal_byte_array (DECIMAL(8, 10));\n  required int32 date_int_32 (DATE);\n  required int32 date_int_64 (DATE);\n  optional int32 date_time (DATE);\n  required int32 time_millis_int_32 (TIME(MILLIS, false));\n  required int32 time_millis_int (TIME(MILLIS, false));\n  required int32 time_millis_int_64 (TIME(MILLIS, true));\n  required int64 time_micros_int_64 (TIME(MICROS, true));\n  required int64 time_nanos_int_64 (TIME(NANOS, true));\n  required int64 time_default_int_64 (TIME(NANOS, false));\n  required int32 time_millis_time (TIME(MILLIS, false));\n  required int64 time_micros_time (TIME(MICROS, true));\n  required int64 time_nanos_time (TIME(NANOS, true));\n  required int64 time_default_time (TIME(NANOS, false));\n  required int64 ts_millis_int_64 (TIMESTAMP(MILLIS, true));\n  required int64 ts_micros_int_64 (TIMESTAMP(MICROS, true));\n  optional int64 ts_nanos_int_64 (TIMESTAMP(NANOS, true));\n  required int64 ts_default_int_64 (TIMESTAMP(NANOS, false));\n  required int64 ts_millis_time (TIMESTAMP(MILLIS, false));\n  required int64 ts_micros_time (TIMESTAMP(MICROS, true));\n  required int64 ts_nanos_time (TIMESTAMP(NANOS, true));\n  required int64 ts_default_time (TIMESTAMP(NANOS, false));\n  required binary json_byte_slice (JSON);\n  required binary json_string (JSON);\n  required binary bson_byte_slice (BSON);\n  required binary bson_string (BSON);\n  required fixed_len_byte_array(16) uuid_byte_array (UUID);\n  required group list_int_64 (LIST) {\n    repeated group list {\n      required int64 element (INT(64, true));\n    }\n  }\n  required group list_decimal_byte_slice (LIST) {\n    repeated group list {\n      required binary element (DECIMAL(100, 10));\n    }\n  }\n  required group list_time_millis_time (LIST) {\n    repeated group list {\n      required int32 element (TIME(MILLIS, true));\n    }\n  }\n  optional group map_string_int_64 (MAP) {\n    repeated group key_value (MAP_KEY_VALUE) {\n      required binary key (STRING);\n      required int64 value (INT(64, true));\n    }\n  }\n  optional group map_decimal_time (MAP) {\n    repeated group key_value (MAP_KEY_VALUE) {\n      required int64 key (DECIMAL(15, 5));\n      required int32 value (TIME(MILLIS, false));\n    }\n  }\n  required group struct {\n    optional int64 int_64 (INT(64, true));\n    required int32 time_millis_int_32 (TIME(MILLIS, false));\n    required group list_time_millis_time (LIST) {\n      repeated group list {\n        required int32 element (TIME(MILLIS, true));\n      }\n    }\n  }\n  required group list_struct (LIST) {\n    repeated group list {\n      required group element {\n        required binary string_byte_slice (STRING);\n        optional int32 date_int_32 (DATE);\n      }\n    }\n  }\n  optional group list_map_date_string (LIST) {\n    repeated group list {\n      required group element (MAP) {\n        repeated group key_value (MAP_KEY_VALUE) {\n          required int64 key (TIME(NANOS, true));\n          required binary value (STRING);\n        }\n      }\n    }\n  }\n  optional group map_string_list_time (MAP) {\n    repeated group key_value (MAP_KEY_VALUE) {\n      required binary key (STRING);\n      required group value (LIST) {\n        repeated group list {\n          required int32 element (TIME(MILLIS, true));\n        }\n      }\n    }\n  }\n}\n",
		},
		"byte array": {
			Input: (*struct {
				Foo [23]byte
				Bar *[42]byte
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  required fixed_len_byte_array(23) foo;\n  optional fixed_len_byte_array(42) bar;\n}\n",
		},
		"simple map": {
			Input: (*struct {
				Foo map[string]int64
			})(nil),
			ExpectErr:      false,
			ExpectedOutput: "message autogen_schema {\n  optional group foo (MAP) {\n    repeated group key_value (MAP_KEY_VALUE) {\n      required binary key (STRING);\n      required int64 value (INT(64, true));\n    }\n  }\n}\n",
		},
		"chan": {
			Input: (*struct {
				Foo chan int
			})(nil),
			ExpectErr: true,
		},
		"func": {
			Input: (*struct {
				Foo func()
			})(nil),
			ExpectErr: true,
		},
		"interface": {Input: (*struct {
			Foo interface{}
		})(nil),
			ExpectErr: true,
		},
		"unsafe.Pointer": {
			Input: (*struct {
				Foo unsafe.Pointer
			})(nil),
			ExpectErr: true,
		},
		"complex64": {
			Input: (*struct {
				Foo complex64
			})(nil),
			ExpectErr: true,
		},
		"complex128": {
			Input: (*struct {
				Foo complex128
			})(nil),
			ExpectErr: true,
		},
		"uintptr": {
			Input: (*struct {
				Foo uintptr
			})(nil),
			ExpectErr: true,
		},
		"invalid struct within struct": {
			Input: (*struct {
				Foo struct {
					Bar uintptr
				}
			})(nil),
			ExpectErr: true,
		},
		"invalid slice": {
			Input: (*struct {
				Foo []chan int
			})(nil),
			ExpectErr: true,
		},
		"invalid pointer": {
			Input: (*struct {
				Foo *complex128
			})(nil),
			ExpectErr: true,
		},
		"invalid map key": {
			Input: (*struct {
				Foo map[complex128]string
			})(nil),
			ExpectErr: true,
		},
		"invalid map value": {
			Input: (*struct {
				Foo map[string]complex64
			})(nil),
			ExpectErr: true,
		},
		"non-struct input": {
			Input:     int64(42),
			ExpectErr: true,
		},
		"time.Time": {
			Input: (*struct {
				Foo time.Time
			})(nil),
			ExpectedOutput: "message autogen_schema {\n  required int64 foo (TIMESTAMP(NANOS, true));\n}\n",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			output, err := GenerateSchema(testData.Input)
			if testData.ExpectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testData.ExpectedOutput, output.String())
			}
		})
	}
}
