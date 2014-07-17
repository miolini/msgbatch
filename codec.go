/*******************************************************************************
 * Copyright 2014 by Artem Andreenko, Openstat.
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
)

type Batch struct {
	Length  int
	Columns []string
	Values  [][]Value
}

type Value struct {
	i int
	v string
}

func (batch *Batch) Add(e map[string]string) {
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
			batch.Values = append(batch.Values, []Value{})
		}
		batch.Values[columnIndex] = append(batch.Values[columnIndex], Value{i: batch.Length, v: value})
	}
	batch.Length++
}

func (batch *Batch) GetValues() (values []map[string]string) {
	for i := 0; i < batch.Length; i++ {
		values = append(values, make(map[string]string))
	}
	for columnIndex, column := range batch.Values {
		columnName := batch.Columns[columnIndex]
		for _, value := range column {
			values[value.i][columnName] = value.v
		}
	}
	return
}

func (batch *Batch) Encode() (data []byte, err error) {
	data, err = msgpack.Marshal(batch)
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
	err = msgpack.Unmarshal(data, &batch)
	return
}
