# StreamKV (beta version)

StreamKV is a streaming key-value store built on top of [Apache Flink](http://flink.apache.org/) to be used within streaming applications requiring more complex stateful logic. StreamKV integrates seamlessly with the Flink fault-tolerance model providing exactly-once semantics.

Key-value operations (put, get, remove...) and their outputs are represented as native Flink `DataStreams` and can be embedded in any Flink Streaming application abstracting away fault-tolerant state sharing between different components. 

StreamKV also supports timestamped operations, which will be executed in an ordered manner using Flink's watermark mechanism.

**Supported operations**

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 15%">Operation</th>
      <th class="text-left" style="width: 50%">Description</th>
      <th class="text-left" style="width: 35%">Output</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td align="center"><strong>Put</strong></td>
      <td>Put a stream of (Key,Value) pairs into the store</td>
      <td>No output</td>
    </tr>
    <tr>
      <td align="center"><strong>Update</strong></td>
      <td>Combine a stream of (Key, Value) pairs with their current value in the store</td>
      <td>No output</td>
    </tr>
    <tr>
      <td align="center"><strong>Get</strong></td>
      <td>Get a stream of Keys from the store</td>
      <td>(Key, Value) Stream</td>
    </tr>
    <tr>
      <td align="center"><strong>Get (KeySelector)</strong></td>
      <td>Get a stream of Keys using a custom key selector for extracting the keys</td>
      <td>(Object, Value) Stream</td>
    </tr>
    <tr>
      <td align="center"><strong>Remove</strong></td>
      <td>Remove a stream of Keys from the store</td>
      <td>(Key, Value) Stream</td>
    </tr>
    <tr>
      <td align="center"><strong>MultiGet</strong></td>
      <td>Get multiple Keys from the store using a stream of Key arrays</td>
      <td>(Key, Value) Array Stream</td>
    </tr>
    <tr>
      <td align="center"><strong>MultiGet (KeySelector)</strong></td>
      <td>Get multiple Keys from the store using a stream of Object arrays and a key selector</td>
      <td>(Object, Value) Array Stream</td>
    </tr>
  </tbody>
</table>


Check out the [Scala](https://github.com/gyfora/StreamKV/blob/master/streamkv-scala/src/main/scala/streamkv/api/scala/example/StreamKVExample.scala) and [Java](https://github.com/gyfora/StreamKV/blob/master/streamkv-java/src/main/java/streamkv/api/java/examples/StreamKVExample.java) example programs! 

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

## API

StreamKV currently offers APIs both for Scala and Java.

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
store.update(transferStream.flatMap(x => List((x._1, -1 * x._3), (x._2, x._3))))(_ + _)

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

## Support

If you have any questions don't hesitate to contact [me](mailto:gyfora@apache.org)!

## Contributors

List of people who have contributed to this project
* Gyula Fóra
* Márton Balassi
* Paris Carbone
