package tarantool

import (
	"context"
	"fmt"
	"time"
)

// Schema contains information about spaces and indexes.
type Schema struct {
	// Spaces is map from space names to spaces.
	Spaces map[string]*Space
	// SpacesByID is map from space numbers to spaces.
	SpacesByID map[uint32]*Space
}

// Space contains information about tarantool space.
type Space struct {
	ID        uint32
	Name      string
	Engine    string
	Temporary bool

	// Field configuration is not mandatory and not checked by tarantool.
	FieldsCount uint32
	Fields      map[string]*Field
	FieldsByID  map[uint32]*Field

	// Indexes is map from index names to indexes.
	Indexes map[string]*Index
	// IndexesByID is map from index numbers to indexes.
	IndexesByID map[uint32]*Index
}

type Field struct {
	ID   uint32
	Name string
	Type string
}

// Index contains information about index.
type Index struct {
	ID     uint32
	Name   string
	Type   string
	Unique bool
	Fields []*IndexField
}

type IndexField struct {
	ID   uint32
	Type string
}

const (
	maxSchemas = 10000
	vSpaceSpID = 281
	vIndexSpID = 289
)

func (conn *Connection) loadSchema() (err error) {
	var resp *Response

	schema := new(Schema)
	schema.SpacesByID = make(map[uint32]*Space)
	schema.Spaces = make(map[string]*Space)

	// Reload spaces.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err = conn.ExecContext(ctx, Select(vSpaceSpID, 0, 0, maxSchemas, IterAll, []interface{}{}))
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		space := new(Space)
		space.ID = uint32(row[0].(uint64))
		space.Name = row[2].(string)
		space.Engine = row[3].(string)
		space.FieldsCount = uint32(row[4].(int64))
		if len(row) >= 6 {
			switch row5 := row[5].(type) {
			case string:
				space.Temporary = row5 == "temporary"
			case map[interface{}]interface{}:
				if temp, ok := row5["temporary"]; ok {
					space.Temporary = temp.(bool)
				}
			default:
				panic("unexpected schema format (space flags)")
			}
		}
		space.FieldsByID = make(map[uint32]*Field)
		space.Fields = make(map[string]*Field)
		space.IndexesByID = make(map[uint32]*Index)
		space.Indexes = make(map[string]*Index)
		if len(row) >= 7 {
			for i, f := range row[6].([]interface{}) {
				if f == nil {
					continue
				}
				f := f.(map[interface{}]interface{})
				field := new(Field)
				field.ID = uint32(i)
				if name, ok := f["name"]; ok && name != nil {
					field.Name = name.(string)
				}
				if type1, ok := f["type"]; ok && type1 != nil {
					field.Type = type1.(string)
				}
				space.FieldsByID[field.ID] = field
				if field.Name != "" {
					space.Fields[field.Name] = field
				}
			}
		}

		schema.SpacesByID[space.ID] = space
		schema.Spaces[space.Name] = space
	}

	// Reload indexes.
	ctx, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	resp, err = conn.ExecContext(ctx, Select(vIndexSpID, 0, 0, maxSchemas, IterAll, []interface{}{}))
	if err != nil {
		return err
	}
	for _, row := range resp.Data {
		row := row.([]interface{})
		index := new(Index)
		index.ID = uint32(row[1].(int64))
		index.Name = row[2].(string)
		index.Type = row[3].(string)
		switch row[4].(type) {
		case uint64:
			index.Unique = row[4].(uint64) > 0
		case map[interface{}]interface{}:
			opts := row[4].(map[interface{}]interface{})
			var ok bool
			if index.Unique, ok = opts["unique"].(bool); !ok {
				/* see bug https://github.com/tarantool/tarantool/issues/2060 */
				index.Unique = true
			}
		default:
			panic("unexpected schema format (index flags)")
		}
		switch fields := row[5].(type) {
		case uint64:
			cnt := int(fields)
			for i := 0; i < cnt; i++ {
				field := new(IndexField)
				field.ID = uint32(row[6+i*2].(int64))
				field.Type = row[7+i*2].(string)
				index.Fields = append(index.Fields, field)
			}
		case []interface{}:
			for _, f := range fields {
				field := new(IndexField)
				switch f := f.(type) {
				case []interface{}:
					field.ID = uint32(f[0].(int64))
					field.Type = f[1].(string)
				case map[interface{}]interface{}:
					field.ID = uint32(f["field"].(int64))
					field.Type = f["type"].(string)
				}
				index.Fields = append(index.Fields, field)
			}
		default:
			panic("unexpected schema format (index fields)")
		}
		spaceID := uint32(row[0].(uint64))
		schema.SpacesByID[spaceID].IndexesByID[index.ID] = index
		schema.SpacesByID[spaceID].Indexes[index.Name] = index
	}
	conn.schema = schema
	return nil
}

func (schema *Schema) resolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error) {
	var space *Space
	var index *Index
	var ok bool

	switch s := s.(type) {
	case string:
		if schema == nil {
			err = fmt.Errorf("schema is not loaded")
			return
		}
		if space, ok = schema.Spaces[s]; !ok {
			err = fmt.Errorf("there is no space with name %s", s)
			return
		}
		spaceNo = space.ID
	case uint:
		spaceNo = uint32(s)
	case uint64:
		spaceNo = uint32(s)
	case uint32:
		spaceNo = s
	case uint16:
		spaceNo = uint32(s)
	case uint8:
		spaceNo = uint32(s)
	case int:
		spaceNo = uint32(s)
	case int64:
		spaceNo = uint32(s)
	case int32:
		spaceNo = uint32(s)
	case int16:
		spaceNo = uint32(s)
	case int8:
		spaceNo = uint32(s)
	case Space:
		spaceNo = s.ID
	case *Space:
		spaceNo = s.ID
	default:
		panic("unexpected type of space param")
	}

	if i != nil {
		switch i := i.(type) {
		case string:
			if schema == nil {
				err = fmt.Errorf("schema is not loaded")
				return
			}
			if space == nil {
				if space, ok = schema.SpacesByID[spaceNo]; !ok {
					err = fmt.Errorf("there is no space with id %d", spaceNo)
					return
				}
			}
			if index, ok = space.Indexes[i]; !ok {
				err = fmt.Errorf("space %s has not index with name %s", space.Name, i)
				return
			}
			indexNo = index.ID
		case uint:
			indexNo = uint32(i)
		case uint64:
			indexNo = uint32(i)
		case uint32:
			indexNo = i
		case uint16:
			indexNo = uint32(i)
		case uint8:
			indexNo = uint32(i)
		case int:
			indexNo = uint32(i)
		case int64:
			indexNo = uint32(i)
		case int32:
			indexNo = uint32(i)
		case int16:
			indexNo = uint32(i)
		case int8:
			indexNo = uint32(i)
		case Index:
			indexNo = i.ID
		case *Index:
			indexNo = i.ID
		default:
			panic("unexpected type of index param")
		}
	}
	return
}
