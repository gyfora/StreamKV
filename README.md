# StreamKV (alpha version)

StreamKV is a streaming key-value store built on top of [Apache Flink](http://flink.apache.org/) to be used within streaming applications requiring more complex stateful logic. StreamKV integrates seamlessly with the Flink fault-tolerance model providing exactly-once semantics.

Key-value operations (put, get, remove...) and their outputs are represented as native Flink `DataStreams` and can be embedded in any Flink Streaming application abstracting away fault-tolerant state sharing between different components. 

StreamKV also supports timestamped operations, which will be executed in an ordered manner using Flink's watermark mechanism.

```java
// Create a new KV store
KVStore<String, Integer> store = new AsyncKVStore<>();

// Create query streams
DataStream<Tuple2<String, Integer>> putStream = ...
DataStream<String> getStream = ...
DataStream<String[]> multiGetStream = ...

// Apply the query streams to the KV store
store.put(putStream);
int id1 = store.get(getStream);
int id2 = store.multiGet(multiGetStream);

// Finalise the KV store operations and get the result streams
KVStoreOutput<String, Integer> storeOutputs = store.getOutputs();

// Get and print the result streams
storeOutputs.getKVStream(id1).print();
storeOutputs.getKVArrayStream(id2).print();
```

**Supported operations**
* Put
* Remove
* Get
* GetWithKeySelector
* MultiGet

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

## Support

If you have any questions don't hesitate to contact [me](mailto:gyfora@apache.org)!

## Contributors

List of people who have contributed to this project
* Gyula Fóra
* Márton Balassi
* Paris Carbone
