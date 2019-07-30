package go_parquet

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/fraugster/parquet-go/parquet"
)

type column struct {
	index          int
	name, flatName string
	// one of the following could be not null. data or children
	data     ColumnStore
	children []*column

	rep parquet.FieldRepetitionType

	maxR, maxD uint16

	// for the reader we should read this element from the meta, for the writer we need to build this element
	element *parquet.SchemaElement
}

func (c *column) MaxDefinitionLevel() uint16 {
	return c.maxD
}

func (c *column) MaxRepetitionLevel() uint16 {
	return c.maxR
}

func (c *column) FlatName() string {
	return c.flatName
}

func (c *column) Name() string {
	return c.name
}

func (c *column) Index() int {
	return c.index
}

func (c *column) Element() *parquet.SchemaElement {
	if c.element == nil {
		c.buildElement()
	}
	return c.element
}

func (c *column) buildElement() {
	rep := c.rep
	elem := &parquet.SchemaElement{
		RepetitionType: &rep,
		Name:           c.name,
		FieldID:        nil, // Not sure about this field
	}

	if c.data != nil {
		t := c.data.parquetType()
		elem.Type = &t
		elem.TypeLength = c.data.typeLen()
		elem.ConvertedType = c.data.convertedType()
		elem.Scale = c.data.scale()
		elem.Precision = c.data.precision()
		elem.LogicalType = c.data.logicalType()
	} else {
		nc := int32(len(c.children))
		elem.NumChildren = &nc
	}

	c.element = elem
}

type Schema struct {
	children []*column

	numRecords int32
}

func (r *Schema) Columns() Columns {
	var ret []Column
	var fn func([]*column)

	fn = func(columns []*column) {
		for i := range columns {
			if columns[i].data != nil {
				ret = append(ret, columns[i])
			} else {
				fn(columns[i].children)
			}
		}
	}

	fn(r.children)
	return ret
}

func (r *Schema) GetColumnByName(path string) Column {
	var fn func([]*column) *column

	fn = func(columns []*column) *column {
		for i := range columns {
			if columns[i].data != nil {
				if columns[i].flatName == path {
					return columns[i]
				}
			} else {
				if c := fn(columns[i].children); c != nil {
					return c
				}
			}
		}

		return nil
	}

	return fn(r.children)
}

// resetData is useful for resetting data after writing a chunk, to collect data for the next chunk
func (r *Schema) resetData() {
	var fn func(c []*column)

	fn = func(c []*column) {
		for i := range c {
			if c[i].children != nil {
				fn(c[i].children)
			} else {
				c[i].data.reset(c[i].data.repetitionType())
			}
		}
	}

	fn(r.children)
}

func (r *Schema) sortIndex() {
	var (
		idx int
		fn  func(c *[]*column)
	)

	fn = func(c *[]*column) {
		if c == nil {
			return
		}
		for data := range *c {
			if (*c)[data].data != nil {
				(*c)[data].index = idx
				idx++
			} else {
				fn(&(*c)[data].children)
			}
		}
	}

	fn(&r.children)
}

// AddGroup add a group to the parquet schema, path is the dot separated path of the group,
// the parent group should be there or it will return an error
func (r *Schema) AddGroup(path string, rep parquet.FieldRepetitionType) error {
	return r.addColumnOrGroup(path, nil, rep)
}

// AddColumn is for adding a column to the parquet schema, it resets the store
// path is the dot separated path of the group, the parent group should be there or it will return an error
func (r *Schema) AddColumn(path string, store ColumnStore, rep parquet.FieldRepetitionType) error {
	if store == nil {
		return errors.New("column should have column store")
	}
	store.reset(rep)
	return r.addColumnOrGroup(path, store, rep)
}

// do not call this function externally
func (r *Schema) addColumnOrGroup(path string, store ColumnStore, rep parquet.FieldRepetitionType) error {
	var (
		maxR, maxD uint16
	)
	if r.children == nil {
		r.children = []*column{}
	}
	pa := strings.Split(path, ".")
	name := strings.Trim(pa[len(pa)-1], " \n\r\t")
	if name == "" {
		return errors.Errorf("the name of the column is required")
	}

	c := &r.children
	for i := 0; i < len(pa)-1; i++ {
		found := false
		if c == nil {
			break
		}
		for j := range *c {
			if (*c)[j].name == pa[i] {
				found = true
				maxR = (*c)[j].maxR
				maxD = (*c)[j].maxD
				c = &(*c)[j].children
				break
			}
		}
		if !found {
			return errors.Errorf("path %s failed on %q", path, pa[i])
		}
		if c == nil && i < len(pa)-1 {
			return errors.Errorf("path %s is not parent at %q", path, pa[i])
		}
	}

	if c == nil {
		return errors.New("the children are nil")
	}
	if rep != parquet.FieldRepetitionType_REQUIRED {
		maxD++
	}
	if rep == parquet.FieldRepetitionType_REPEATED {
		maxR++
	}

	col := &column{
		name:     name,
		flatName: path,
		data:     nil,
		children: nil,
		rep:      rep,
		maxR:     maxR,
		maxD:     maxD,
	}
	if store != nil {
		store.reset(rep)
		col.data = store
	} else {
		col.children = []*column{}
	}

	*c = append(*c, col)
	r.sortIndex()

	return nil
}

func (r *Schema) findDataColumn(path string) (*column, error) {
	pa := strings.Split(path, ".")
	c := r.children
	var ret *column
	for i := 0; i < len(pa); i++ {
		found := false
		for j := range c {
			if c[j].name == pa[i] {
				found = true
				ret = c[j]
				c = c[j].children
				break
			}
		}
		if !found {
			return nil, errors.Errorf("path %s failed on %q", path, pa[i])
		}
		if c == nil && i < len(pa)-1 {
			return nil, errors.Errorf("path %s is not parent at %q", path, pa[i])
		}
	}

	if ret == nil || ret.data == nil {
		return nil, errors.Errorf("path %s doesnt end on data", path)
	}

	return ret, nil
}

func (r *Schema) AddData(m map[string]interface{}) error {
	_, err := recursiveAddColumnData(r.children, m, 0, 0, 0)
	if err != nil {
		r.numRecords++
	}
	return err
}

func recursiveAddColumnNil(c []*column, defLvl, maxRepLvl uint16, repLvl uint16) error {
	for i := range c {
		if c[i].data != nil {
			_, err := c[i].data.add(nil, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return err
			}
		}
		if c[i].children != nil {
			if err := recursiveAddColumnNil(c, defLvl, maxRepLvl, repLvl); err != nil {
				return err
			}
		}
	}
	return nil
}

func recursiveAddColumnData(c []*column, m interface{}, defLvl uint16, maxRepLvl uint16, repLvl uint16) (bool, error) {
	var data = m.(map[string]interface{})
	var advance bool
	for i := range c {
		d := data[c[i].name]
		if c[i].data != nil {
			inc, err := c[i].data.add(d, defLvl, maxRepLvl, repLvl)
			if err != nil {
				return false, err
			}

			if inc {
				advance = true //
			}
		}
		if c[i].children != nil {
			l := defLvl
			// In case of required value, there is no need to add a definition value, since it should be there always,
			// also for nil value, it means we should skip from this level to the lowest level
			if c[i].rep != parquet.FieldRepetitionType_REQUIRED && d != nil {
				l++
			}

			switch v := d.(type) {
			case nil:
				if err := recursiveAddColumnNil(c[i].children, l, maxRepLvl, repLvl); err != nil {
					return false, err
				}
			case map[string]interface{}: // Not repeated
				if c[i].rep == parquet.FieldRepetitionType_REPEATED {
					return false, errors.Errorf("repeated group should be array")
				}
				_, err := recursiveAddColumnData(c[i].children, v, l, maxRepLvl, repLvl)
				if err != nil {
					return false, err
				}
			case []map[string]interface{}:
				m := maxRepLvl
				if c[i].rep == parquet.FieldRepetitionType_REPEATED {
					m++
				}
				if c[i].rep != parquet.FieldRepetitionType_REPEATED {
					return false, errors.Errorf("no repeated group should not be array")
				}
				rL := repLvl
				for vi := range v {
					inc, err := recursiveAddColumnData(c[i].children, v[vi], l, m, rL)
					if err != nil {
						return false, err
					}

					if inc {
						advance = true
						rL = m
					}
				}

			default:
				return false, errors.Errorf("data is not a map or array of map, its a %T", v)
			}
		}
	}

	return advance, nil
}

func (c *column) readColumnSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
	s := schema[idx]

	// TODO: validate Name is not empty
	if s.RepetitionType == nil {
		return 0, errors.Errorf("field RepetitionType is nil in index %d", idx)
	}

	if *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	c.element = s
	c.maxR = rLevel
	c.maxD = dLevel
	c.data = &genericStore{} // Non nil but invalid store?
	c.flatName = name + "." + s.Name
	if name == "" {
		c.flatName = s.Name
	}
	return idx + 1, nil
}

func (c *column) readGroupSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (int, error) {
	if len(schema) <= idx {
		return 0, errors.New("schema index out of bound")
	}

	s := schema[idx]
	if s.Type != nil {
		return 0, errors.Errorf("field Type is not nil in index %d", idx)
	}
	if s.NumChildren == nil {
		return 0, errors.Errorf("the field NumChildren is invalid in index %d", idx)
	}

	if *s.NumChildren <= 0 {
		return 0, errors.Errorf("the field NumChildren is zero in index %d", idx)
	}
	l := int(*s.NumChildren)

	if len(schema) <= idx+l {
		return 0, errors.Errorf("not enough element in the schema list in index %d", idx)
	}

	if s.RepetitionType != nil && *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if s.RepetitionType != nil && *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	if idx != 0 {
		if name == "" {
			name = s.Name
		} else {
			name += "." + s.Name
		}
	}

	// TODO : Do more validation here
	c.element = s
	c.children = make([]*column, 0, l)

	var err error
	idx++ // move idx from this group to next
	for i := 0; i < l; i++ {
		if schema[idx].Type == nil {
			// another group
			child := &column{}
			idx, err = child.readGroupSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}
			c.children = append(c.children, child)
		} else {
			child := &column{}
			idx, err = child.readColumnSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}
			c.children = append(c.children, child)
		}
	}

	return idx, nil
}

func (r *Schema) readSchema(schema []*parquet.SchemaElement) error {
	var err error
	for idx := 0; idx < len(schema); {
		c := &column{}
		if schema[idx].Type == nil {
			idx, err = c.readGroupSchema(schema, "", idx, 0, 0)
			if err != nil {
				return err
			}
			r.children = append(r.children, c)
		} else {
			idx, err = c.readColumnSchema(schema, "", idx, 0, 0)
			if err != nil {
				return err
			}
			r.children = append(r.children, c)
		}
	}
	r.sortIndex()
	return nil
}

func MakeSchema(meta *parquet.FileMetaData) (*Schema, error) {
	s := &Schema{
		children: make([]*column, 0, len(meta.Schema)-1),
	}
	err := s.readSchema(meta.Schema)
	if err != nil {
		return nil, err
	}

	return s, nil
}
