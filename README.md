# StreamKV (alpha version)

StreamKV is a streaming key-value store built on top of [Apache Flink](http://flink.apache.org/) to be used within streaming applications requiring more complex stateful logic. StreamKV integrates seamlessly with the Flink fault-tolerance model providing exactly-once semantics.

Key-value operations (put, get, remove...) and their outputs are represented as native Flink `DataStreams` and can be embedded in any Flink Streaming application abstracting away fault-tolerant state sharing between different components. 

StreamKV also supports timestamped operations, which will be executed in an ordered manner using Flink's watermark mechanism.

**Scala API**

```scala
case class Person(id: Int, name: String, age: Int)

// Create a store with arrival time ordering
val store = KVStore[Int, Person](PARTIAL)

// Fill the store with people
val persons: DataStream[Person] = …
store.put(persons.map(p => (p.id, p)))

// Query the KV Store using the IDs
val ids : DataStream[Int] = …
val personQuery = store.get(ids)

// Compute the age difference for ID pairs
val idPairs : DataStream[(Int, Int)] = …
// Get both ages at the same type using multiGet, we compute the diff later
val pairQuery = store.multiGet(idPairs.map(pair => Array(pair._1,pair._2)))

// Print the query results
personQuery.getOutput.print
pairQuery.getOutput.map(a =>((a(0)._1, a(1)._1), Math.abs(a(0)._2.age - a(1)._2.age))).print
```

**Java API**

```java
// Create a new KV store
KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.PARTIAL);

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
