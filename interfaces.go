package go_parquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// Column is one column definition in the parquet file
type Column interface {
	// Index of the column in the schema
	Index() int
	// Name of the column
	Name() string
	// Name of the column with the name of parent structures, separated with dot
	FlatName() string
	// MaxDefinitionLevel of the column
	MaxDefinitionLevel() uint16
	// MaxRepetitionLevel of the column
	MaxRepetitionLevel() uint16
	// Element of the column in the schema
	Element() *parquet.SchemaElement

	getColumnStore() *ColumnStore
}

// Columns array of the column
type Columns []Column

// pageReader is an internal interface used only internally to read the pages
type pageReader interface {
	init(dDecoder, rDecoder getLevelDecoder, values getValueDecoderFn) error
	read(r io.ReadSeeker, ph *parquet.PageHeader, codec parquet.CompressionCodec) error

	readValues([]interface{}) (n int, dLevel []int32, rLevel []int32, err error)

	numValues() int32
}

type valuesDecoder interface {
	init(io.Reader) error
	// the error io.EOF with the less value is acceptable, any other error is not
	decodeValues([]interface{}) (int, error)
}

type dictValuesDecoder interface {
	valuesDecoder

	setValues([]interface{})
}

type valuesEncoder interface {
	init(io.Writer) error
	encodeValues([]interface{}) error

	io.Closer
}

type dictValuesEncoder interface {
	valuesEncoder

	getValues() []interface{}
}

// parquetColumn is to convert a store to a parquet.SchemaElement
type parquetColumn interface {
	parquetType() parquet.Type
	typeLen() *int32
	repetitionType() parquet.FieldRepetitionType
	convertedType() *parquet.ConvertedType
	scale() *int32
	precision() *int32
	logicalType() *parquet.LogicalType
}

type typedColumnStore interface {
	parquetColumn
	reset(repetitionType parquet.FieldRepetitionType)
	// Min and Max in parquet byte
	maxValue() []byte
	minValue() []byte

	// Should extract the value, turn it into an array and check for min and max on all values in this
	getValues(v interface{}) ([]interface{}, error)
	sizeOf(v interface{}) int
	// the tricky append. this is a way of creating new "typed" array. the first interface is nil or an []T (T is the type,
	// not the interface) and value is from that type. the result should be always []T (array of that type)
	// exactly like the builtin append
	append(arrayIn interface{}, value interface{}) interface{}
}
