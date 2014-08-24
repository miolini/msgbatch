/*******************************************************************************
 * Copyright 2014 by Artem Andreenko.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
 package msgbatch

import (
	"code.google.com/p/snappy-go/snappy"
	"github.com/vmihailenco/msgpack"
	"fmt"
)

type Batch struct {
	Length  int64
	Columns []string
	Values  [][][]interface{}
}

func (batch *Batch) Add(e map[string]interface{}) {
	for key, value := range e {
		columnIndex := -1
		for i, column := range batch.Columns {
			if column == key {
				columnIndex = i
				break
			}
		}
		if columnIndex == -1 {
			columnIndex = len(batch.Columns)
			batch.Columns = append(batch.Columns, key)
			batch.Values = append(batch.Values, [][]interface{}{})
		}
		batch.Values[columnIndex] = append(batch.Values[columnIndex], []interface{}{batch.Length, value})
	}
	batch.Length++
}

func (batch *Batch) GetValues() (values []map[string]interface{}) {
	for i := int64(0); i < batch.Length; i++ {
		values = append(values, make(map[string]interface{}))
	}
	for columnIndex, column := range batch.Values {
		columnName := batch.Columns[columnIndex]
		for _, value := range column {
			values[value[0].(int64)][columnName] = value[1]
		}
	}
	return
}

func (batch *Batch) Encode() (data []byte, err error) {
	rawBatch := make([]interface{}, 3)
	rawBatch[0] = batch.Length
	rawBatch[1] = batch.Columns
	rawBatch[2] = batch.Values
	data, err = msgpack.Marshal(rawBatch)
	if err != nil {
		return
	}
	data, err = snappy.Encode(nil, data)
	return
}

func Decode(data []byte) (batch Batch, err error) {
	data, err = snappy.Decode(nil, data)
	if err != nil {
		return
	}
	var rawBatch []interface{}
	err = msgpack.Unmarshal(data, &rawBatch)
	if len(rawBatch) != 3 {
		err = fmt.Errorf("bad batch data: root columns not equals 3: %d", len(rawBatch))
		return
	}
	batch.Length = rawBatch[0].(int64)
	columns := rawBatch[1].([]interface{})
	for _, column := range columns {
		batch.Columns = append(batch.Columns, column.(string))
	}
	valueColumns := rawBatch[2].([]interface{})
	for _, valueColumn := range valueColumns {
		bValueColumn := [][]interface{}{}
		for _, _values := range valueColumn.([]interface{}) {
			values := _values.([]interface{})
			bValueColumn = append(bValueColumn, values)
		}
		batch.Values = append(batch.Values, bValueColumn)
	}
	return
}
