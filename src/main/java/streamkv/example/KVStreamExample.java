/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamkv.example;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import streamkv.api.KVStore;
import streamkv.api.KVStore.OperationOrdering;
import streamkv.api.KVStoreOutput;

/**
 * This example shows an implementation of a key value store with operations from
 * text sockets. To run the example make sure that the service providing the text
 * data is already up and running.
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number. Make sure to start the services for both port 9999 (for put),
 * 9998 (for get) and 9997 (for multiget).
 *
 * This example shows how to:
 * <ul>
 * <li>use the {@link KVStore} abstraction
 * <li>put to the key-value store,
 * <li>get and multiget from the key-value store.
 * </ul>
 *
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class KVStreamExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Create a new KV store
		KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.PARTIAL);

		// Create query streams
		DataStream<Tuple2<String, Integer>> putStream = env.socketTextStream("localhost", 9999).flatMap(
				new Parser());
		DataStream<String> getStream1 = env.socketTextStream("localhost", 9998);
		DataStream<String[]> getStream2 = env.socketTextStream("localhost", 9997).flatMap(new KArrayParser());

		// Apply the query streams to the KV store
		store.put(putStream);
		int id1 = store.get(getStream1);
		int id2 = store.multiGet(getStream2);

		// Finalize the KV store operations and get the result streams
		KVStoreOutput<String, Integer> storeOutputs = store.getOutputs();

		// Fetch the result streams for the 2 get queries using the assigned IDs
		// and print the results
		storeOutputs.getKVStream(id1).print();
		storeOutputs.getKVArrayStream(id2).addSink(new PrintArray());

		env.execute();
	}

	public static class Parser implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			try {
				String[] split = value.split(",");
				out.collect(Tuple2.of(split[0], Integer.valueOf(split[1])));
			} catch (Exception e) {

			}
		}
	}

	public static class KArrayParser implements FlatMapFunction<String, String[]> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<String[]> out) throws Exception {
			try {
				out.collect(value.split(","));
			} catch (Exception e) {

			}
		}
	}

	public static class PrintArray implements SinkFunction<Tuple2<String, Integer>[]> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple2<String, Integer>[] value) throws Exception {
			System.out.println(Arrays.toString(value));
		}

	}
}
