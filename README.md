msgbatch
========

Go lang msgpack batch data library

Examples:

Encode:
```
 batch := msgbatch.Batch{}
 batch.Add(map[string]string{"url":"http://google.com/", "ts":"123"})
 batch.Add(map[string]string{"url":"http://google.com/", "id":"908908"})
 batch.Add(map[string]string{"url":"http://google.com/", "ts":"43556478346", "id": "6546"})
 batch.Add(map[string]string{"url":"http://google.com/"})
 dataBytes, err := batch.Encode()
 log.Printf("data len %d, err %s", len(dataBytes), err) 
```

Decode:
```
 batch, err = msgbatch.Decode(dataBytes)
 log.Printf("batch %v", batch.GetValues())
```

