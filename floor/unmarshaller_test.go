package floor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectUnmarshalling(t *testing.T) {
	obj := newObjectWithData(map[string]interface{}{
		"foo":  int64(23),
		"bar":  int32(42),
		"baz":  true,
		"name": []byte("John Doe"),
		"my_group": map[string]interface{}{
			"foo1": float32(23.5),
			"bar1": float64(9000.5),
		},
		"id_list": map[string]interface{}{
			"list": []map[string]interface{}{
				{
					"element": int64(1),
				},
				{
					"element": int64(2),
				},
				{
					"element": int64(15),
				},
				{
					"element": int64(28),
				},
				{
					"element": int64(32),
				},
			},
		},
		"data_map": map[string]interface{}{
			"key_value": []map[string]interface{}{
				{
					"key":   []byte("data0"),
					"value": int32(0),
				},
				{
					"key":   []byte("data1"),
					"value": int32(1),
				},
				{
					"key":   []byte("data2"),
					"value": int32(2),
				},
				{
					"key":   []byte("data3"),
					"value": int32(3),
				},
				{
					"key":   []byte("data4"),
					"value": int32(4),
				},
			},
		},
		"nested_data_map": map[string]interface{}{
			"key_value": []map[string]interface{}{
				{
					"key": int64(23),
					"value": map[string]interface{}{
						"foo": int32(42),
					},
				},
			},
		},
	})

	elem, err := obj.GetField("foo")
	require.NoError(t, err, "getting foo failed")
	i64, err := elem.Int64()
	require.NoError(t, err, "getting foo as int64 failed")
	require.Equal(t, int64(23), i64)

	elem, err = obj.GetField("bar")
	require.NoError(t, err, "getting bar failed")
	i32, err := elem.Int32()
	require.NoError(t, err, "getting bar as int32 failed")
	require.Equal(t, int32(42), i32)

	elem, err = obj.GetField("baz")
	require.NoError(t, err, "getting baz failed")
	b, err := elem.Bool()
	require.NoError(t, err, "getting baz as bool failed")
	require.Equal(t, true, b)

	elem, err = obj.GetField("my_group")
	require.NoError(t, err, "getting my_group failed")
	myGroup, err := elem.Group()
	require.NoError(t, err, "getting my_group as group failed")

	elem, err = myGroup.GetField("foo1")
	require.NoError(t, err, "getting my_group.foo1 failed")
	f32, err := elem.Float32()
	require.NoError(t, err, "getting my_group.foo1 as float32 failed")
	require.Equal(t, float32(23.5), f32)

	elem, err = myGroup.GetField("bar1")
	require.NoError(t, err, "getting my_group.bar1 failed")
	f64, err := elem.Float64()
	require.NoError(t, err, "getting my_group.bar1 as float64 failed")
	require.Equal(t, float64(9000.5), f64)

	elem, err = obj.GetField("id_list")
	require.NoError(t, err, "getting id_list failed")
	idList, err := elem.List()
	require.NoError(t, err, "getting id_list as list failed")

	var i64List []int64

	for idList.Next() {
		v, err2 := idList.Value()
		require.NoError(t, err2, "getting list value failed")
		i64, err3 := v.Int64()
		require.NoError(t, err3, "getting list value as int64 failed")
		i64List = append(i64List, i64)
	}

	require.Equal(t, []int64{1, 2, 15, 28, 32}, i64List, "list id_list values don't match")

	elem, err = obj.GetField("data_map")
	require.NoError(t, err, "getting data_map failed")
	dataMap, err := elem.Map()
	require.NoError(t, err, "getting data_map as map failed")

	mapData := make(map[string]int32)

	for dataMap.Next() {
		key, err := dataMap.Key()
		require.NoError(t, err, "getting key from map failed")
		keyData, err := key.ByteArray()
		require.NoError(t, err, "getting key as []byte failed")

		value, err := dataMap.Value()
		require.NoError(t, err, "getting value from map failed")
		valueData, err := value.Int32()
		require.NoError(t, err, "getting value as int32 failed")

		mapData[string(keyData)] = valueData
	}

	require.Equal(t, map[string]int32{"data0": 0, "data1": 1, "data2": 2, "data3": 3, "data4": 4}, mapData)
}

func TestObjectUnmarshallingErrors(t *testing.T) {
	obj := newObjectWithData(map[string]interface{}{
		"foo":          int64(23),
		"bar":          int32(42),
		"invalid_list": map[string]interface{}{},
		"invalid_list_2": map[string]interface{}{
			"list": map[string]interface{}{"foo": int32(0)},
		},
		"invalid_list_element": map[string]interface{}{
			"list": []map[string]interface{}{
				{"foo": int32(0)},
			},
		},
		"invalid_map": map[string]interface{}{},
		"invalid_map_2": map[string]interface{}{
			"key_value": map[string]interface{}{"foo": int32(0)},
		},

		"data_map_no_keyvalues": map[string]interface{}{
			"key_value": []map[string]interface{}{
				{},
			},
		},
	})

	_, err := obj.GetField("does_not_exist")
	require.Error(t, err)

	elem, err := obj.GetField("foo")
	require.NoError(t, err)
	_, err = elem.Bool()
	require.Error(t, err)
	_, err = elem.ByteArray()
	require.Error(t, err)
	_, err = elem.Float32()
	require.Error(t, err)
	_, err = elem.Float64()
	require.Error(t, err)
	_, err = elem.Int32()
	require.Error(t, err)
	_, err = elem.Group()
	require.Error(t, err)
	_, err = elem.List()
	require.Error(t, err)
	_, err = elem.Map()
	require.Error(t, err)

	elem, err = obj.GetField("bar")
	require.NoError(t, err)
	_, err = elem.Int64()
	require.Error(t, err)

	elem, err = obj.GetField("invalid_list")
	require.NoError(t, err)
	_, err = elem.List()
	require.Error(t, err)

	elem, err = obj.GetField("invalid_list_2")
	require.NoError(t, err)
	_, err = elem.List()
	require.Error(t, err)

	elem, err = obj.GetField("invalid_list_element")
	require.NoError(t, err)
	list, err := elem.List()

	for list.Next() {
		_, err2 := list.Value()
		require.Error(t, err2)
	}

	_, err = list.Value()
	require.Error(t, err)

	elem, err = obj.GetField("invalid_map")
	require.NoError(t, err)
	_, err = elem.Map()
	require.Error(t, err)

	elem, err = obj.GetField("invalid_map_2")
	require.NoError(t, err)
	_, err = elem.Map()
	require.Error(t, err)

	elem, err = obj.GetField("data_map_no_keyvalues")
	require.NoError(t, err)
	dataMap, err := elem.Map()
	require.NoError(t, err)

	for dataMap.Next() {
		_, err = dataMap.Key()
		require.Error(t, err)

		_, err = dataMap.Value()
		require.Error(t, err)
	}

	_, err = dataMap.Key()
	require.Error(t, err)

	_, err = dataMap.Value()
	require.Error(t, err)
}