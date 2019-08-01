package go_parquet

import (
	"io"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

// File is the parquet file
type FileReader struct {
	meta *parquet.FileMetaData
	SchemaReader
	reader io.ReadSeeker
}

// NewFileReader try to create a reader from a stream
func NewFileReader(r io.ReadSeeker) (*FileReader, error) {
	meta, err := ReadFileMetaData(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading file meta data failed")
	}

	schema, err := makeSchema(meta)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}
	// Reset the reader to the beginning of the file
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		return nil, err
	}
	return &FileReader{
		meta:         meta,
		SchemaReader: schema,
		reader:       r,
	}, nil
}

func (f *FileReader) RawGroupCount() int {
	return len(f.meta.RowGroups)
}

