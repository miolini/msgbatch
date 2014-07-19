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
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestCodecDifferent(t *testing.T) {
	batch := Batch{}
	batch.Add(map[string]interface{}{"url":"http://google.com/", "ts":123})
	batch.Add(map[string]interface{}{"url":"http://google.com/", "id":908908})
	batch.Add(map[string]interface{}{"url":"http://google.com/", "ts":43556478346, "id": 6546})
	batch.Add(map[string]interface{}{"url":"http://yahoo.com/"})
	batch.Add(map[string]interface{}{})
	t.Logf("batch %v", batch)
	data, _ := batch.Encode()
	t.Logf("data %d: %s", len(data), string(data))
}

func BenchmarkEncode(b *testing.B) {
	batch := genBatch(1000)
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = batch.Encode()
	}
	if err != nil {
		b.Logf("encode err: %s", err)
	} else {
		b.Logf("data %d", len(data))
	}
}

func BenchmarkDecode(b *testing.B) {
	batch := genBatch(1000)
	data, err := batch.Encode()
	for i := 0; i < b.N; i++ {
		batch, err = Decode(data)
	}
	if err != nil {
		b.Logf("encode err: %s", err)
	} else {
		b.Logf("batch: bytes: %d, length: %d", len(data), len(batch.GetValues()))
	}
}

func genBatch(num int) Batch {
	batch := Batch{}
	for i := 0; i < 1000; i++ {
		e := map[string]interface{}{}
		e["ts"] = fmt.Sprintf("%d", time.Now().Unix())
		e["url"] = fmt.Sprintf("http://%d/%d", rand.Int(), rand.Int())
		if rand.Int() % 3 == 0 {
			e["rf"] = fmt.Sprintf("http://%d/%d", rand.Int(), rand.Int())
		}
		e["rand"] = fmt.Sprintf("%d", rand.Int())
		batch.Add(e)
	}
	return batch
}
