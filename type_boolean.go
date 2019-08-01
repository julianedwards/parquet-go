package go_parquet

import (
	"io"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type booleanPlainDecoder struct {
	r    io.Reader
	left []bool
}

// copy the left overs from the previous call. instead of returning an empty subset of the old slice,
// it delete the slice (by returning nil) so there is no memory leak because of the underlying array
// the return value is the new left over and the number of read message
func copyLeftOvers(dst []interface{}, src []bool) ([]bool, int) {
	size := len(dst)
	var clean bool
	if len(src) <= size {
		size = len(src)
		clean = true
	}

	for i := 0; i < size; i++ {
		dst[i] = src[i]
	}
	if clean {
		return nil, size
	}

	return src[size:], size
}

func (b *booleanPlainDecoder) init(r io.Reader) error {
	b.r = r
	b.left = nil

	return nil
}

func (b *booleanPlainDecoder) decodeValues(dst []interface{}) (int, error) {
	var start int
	if len(b.left) > 0 {
		// there is a leftover from the last run
		b.left, start = copyLeftOvers(dst, b.left)
		if b.left != nil {
			return len(dst), nil
		}
	}

	buf := make([]byte, 1)
	for i := start; i < len(dst); i += 8 {
		if _, err := io.ReadFull(b.r, buf); err != nil {
			return i, err
		}
		d := unpack8int32_1(buf)
		for j := 0; j < 8; j++ {
			if i+j < len(dst) {
				dst[i+j] = d[j] == 1
			} else {
				b.left = append(b.left, d[j] == 1)
			}
		}
	}

	return len(dst), nil
}

type booleanPlainEncoder struct {
	w    io.Writer
	left []interface{}
}

func (b *booleanPlainEncoder) Close() error {
	if len(b.left) == 0 {
		return nil
	}
	data := make([]interface{}, 0, 8)
	data = append(data, b.left...)
	b.left = nil
	for len(data)%8 != 0 {
		data = append(data, false)
	}

	return b.encodeValues(data)
}

func (b *booleanPlainEncoder) init(w io.Writer) error {
	b.w = w
	b.left = nil
	return nil
}

func (b *booleanPlainEncoder) encodeValues(values []interface{}) error {
	if len(b.left) > 0 {
		values = append(b.left, values...)
	}
	lf := len(values)
	l := lf % 8

	buf := make([]byte, 0, lf/8+1)
	for i := 0; i < lf-l; i += 8 {
		var i8 [8]int32
		for j := 0; j < 8; j++ {
			if values[i+j].(bool) {
				i8[j] = 1
			}
		}
		buf = append(buf, pack8int32_1(i8)...)
	}

	if l > 0 {
		b.left = make([]interface{}, l)
		for i := lf - l; i < lf; i++ {
			b.left[i-lf+l] = values[i]
		}
	}

	if len(buf) == 0 {
		return nil
	}
	return writeFull(b.w, buf)
}

type booleanRLEDecoder struct {
	decoder *hybridDecoder
}

func (b *booleanRLEDecoder) init(r io.Reader) error {
	b.decoder = newHybridDecoder(1)
	return b.decoder.initSize(r)
}

func (b *booleanRLEDecoder) decodeValues(dst []interface{}) (int, error) {
	total := len(dst)
	for i := 0; i < total; i += 1 {
		n, err := b.decoder.next()
		if err != nil {
			return i, err
		}
		dst[i] = n == 1
	}

	return total, nil
}

type booleanRLEEncoder struct {
	encoder *hybridEncoder
}

func (b *booleanRLEEncoder) Close() error {
	return b.encoder.Close()
}

func (b *booleanRLEEncoder) init(w io.Writer) error {
	b.encoder = newHybridEncoder(1)
	return b.encoder.initSize(w)
}

func (b *booleanRLEEncoder) encodeValues(values []interface{}) error {
	buf := make([]int32, len(values))
	for i := range values {
		if values[i].(bool) {
			buf[i] = 1
		} else {
			buf[i] = 0
		}
	}

	return b.encoder.encode(buf)
}

type booleanStore struct {
	repTyp parquet.FieldRepetitionType
}

func (b *booleanStore) sizeOf(v interface{}) int {
	// Cheating here. boolean size is one bit, but the size is in byte. so zero to make sure
	// we never use dictionary on this. // TODO: is this make sense?
	return 0
}

func (b *booleanStore) parquetType() parquet.Type {
	return parquet.Type_BOOLEAN
}

func (b *booleanStore) typeLen() *int32 {
	return nil
}

func (b *booleanStore) repetitionType() parquet.FieldRepetitionType {
	return b.repTyp
}

func (b *booleanStore) convertedType() *parquet.ConvertedType {
	return nil
}

func (b *booleanStore) scale() *int32 {
	return nil
}

func (b *booleanStore) precision() *int32 {
	return nil
}

func (b *booleanStore) logicalType() *parquet.LogicalType {
	return nil
}

func (b *booleanStore) reset(repetitionType parquet.FieldRepetitionType) {
	b.repTyp = repetitionType
}

func (b *booleanStore) maxValue() []byte {
	return nil
}

func (b *booleanStore) minValue() []byte {
	return nil
}

func (b *booleanStore) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case bool:
		vals = []interface{}{typed}
	case []bool:
		if b.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in bool column %T => %+v", v, v)
	}

	return vals, nil
}

func (b *booleanStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]bool, 0, 1)
	}
	return append(arrayIn.([]bool), value.(bool))
}
