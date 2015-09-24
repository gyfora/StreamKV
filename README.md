# StreamKV (alpha version)

StreamKV is a streaming key-value store built on top of [Apache Flink](http://flink.apache.org/) to be used within streaming applications requiring more complex stateful logic. StreamKV integrates seamlessly with the Flink fault-tolerance model providing exactly-once semantics.

Key-value operations (put, get, remove...) and their outputs are represented as native Flink `DataStreams` and can be embedded in any Flink Streaming application abstracting away fault-tolerant state sharing between different components. 

StreamKV also supports timestamped operations, which will be executed in an ordered manner using Flink's watermark mechanism.

**Scala API**

```scala
// Create a store for account information (name, balance)
val store = KVStore[String, Double](ARRIVALTIME)

// Feed the balance stream into the store
val initialBalance : DataStream[(String,Double)] = …
store.put(initialBalance)

// At any time query the balance by name
val names : DataStream[String] = …
val balanceQ = store.get(names)

// At any time query the balance for multiple people
val nameArrays : DataStream[Array[String]] = …
val totalBalanceQ = store.multiGet(nameArrays)

// Transfer : (from, to, amount)
val transferStream: DataStream[(String, String, Double)] = …

// Apply transfer by subtracting from the sender and adding to the receiver
store.update(transferStream.flatMap(x => Array((x._1, -1 * x._3), (x._2, x._3))))((b1, b2) => b1 + b2)

// Print the query outputs
balanceQ.getOutput.print
totalBalanceQ.getOutput.addSink(x => println(x.mkString(",")))
```

**Java API**

```java
// Create a new KV store
KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.ARRIVALTIME);

// Create query streams
DataStream<Tuple2<String, Integer>> putStream = ...
DataStream<String> getStream = ...
DataStream<String[]> multiGetStream = ...

// Apply the query streams to the KV store
store.put(putStream);
Query<Tuple2<String, Integer>> q1 = store.get(getStream);
Query<Tuple2<String, Integer>[]> q2 = store.multiGet(multiGetStream);

// Get and print the result streams
q1.getOutput().print();
q2.getOutput().print();
```

**Supported operations**
* Put
* Remove
* Update
* Get
* GetWithKeySelector
* MultiGet
* MultiGetWithKeySelector

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

## Support

If you have any questions don't hesitate to contact [me](mailto:gyfora@apache.org)!

## Contributors

List of people who have contributed to this project
* Gyula Fóra
* Márton Balassi
* Paris Carbone
