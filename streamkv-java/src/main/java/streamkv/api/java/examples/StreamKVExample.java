/*
 * Copyright 2015 Gyula Fóra
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamkv.api.java.examples;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import streamkv.api.java.KVStore;
import streamkv.api.java.OperationOrdering;
import streamkv.api.java.Query;

/**
 * This example shows an example application using the key value store with
 * operations from text sockets. To run the example make sure that the service
 * providing the text data is already up and running.
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>
 * <p>
 * Valid inputs:
 * <ul>
 * <li>"put name, amount"</li>
 * <li>"get name"</li>
 * <li>"mget name1,name2,..."</li>
 * <li>"transfer from, to, amount"</li>
 * </ul>
 * 
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
@SuppressWarnings("serial")
public class StreamKVExample {

	public static void main(String[] args) throws Exception {

		// Get the environment and enable checkpointing
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);

		// Create a store for account information (name, balance)
		KVStore<String, Double> store = KVStore.withOrdering(OperationOrdering.ARRIVALTIME);

		// Read and parse the input stream from the text socket
		DataStream<Tuple2<String, String>> inputStream = env.socketTextStream("localhost", 9999).flatMap(
				new Parser());

		// Convert the put stream to Tuple2-s
		DataStream<Tuple2<String, Double>> initialBalance = selectOp(inputStream, "put").map(
				new MapFunction<String, Tuple2<String, Double>>() {

					@Override
					public Tuple2<String, Double> map(String in) throws Exception {
						String[] split = in.split(",");
						return Tuple2.of(split[0], Double.parseDouble(split[1]));
					}

				});

		// Feed the balance stream into the store
		store.put(initialBalance);

		// At any time query the balance by name
		Query<Tuple2<String, Double>> balanceQ = store.get(selectOp(inputStream, "get"));

		// At any time query the balance for multiple people
		Query<Tuple2<String, Double>[]> mBalanceQ = store.multiGet(selectOp(inputStream, "mget").map(
				new MapFunction<String, String[]>() {

					@Override
					public String[] map(String value) throws Exception {
						return value.split(",");
					}
				}));

		// Parse the transfer stream to (from, to, amount)
		DataStream<Tuple3<String, String, Double>> transferStream = selectOp(inputStream, "transfer").map(
				new MapFunction<String, Tuple3<String, String, Double>>() {

					@Override
					public Tuple3<String, String, Double> map(String value) throws Exception {
						String[] split = value.split(",");
						return Tuple3.of(split[0], split[1], Double.parseDouble(split[2]));
					}
				});

		// Apply transfer by subtracting from the sender and adding to the receiver
		store.update(transferStream
				.flatMap(new FlatMapFunction<Tuple3<String, String, Double>, Tuple2<String, Double>>() {

					@Override
					public void flatMap(Tuple3<String, String, Double> in,
							Collector<Tuple2<String, Double>> out) throws Exception {
						out.collect(Tuple2.of(in.f0, -1 * in.f2));
						out.collect(Tuple2.of(in.f1, in.f2));
					}
				}), new Sum());

		// Print the query outputs
		balanceQ.getOutput().print();
		mBalanceQ.getOutput().map(new ArrayToString()).print();

		// Execute the program
		env.execute();
	}

	/**
	 * Utility for filtering a specific operation stream from the input
	 */
	public static DataStream<String> selectOp(DataStream<Tuple2<String, String>> input, final String op) {
		return input.filter(new FilterFunction<Tuple2<String, String>>() {

			@Override
			public boolean filter(Tuple2<String, String> value) throws Exception {
				return value.f0.equals(op);
			}
		}).<Tuple1<String>> project(1).map(new MapFunction<Tuple1<String>, String>() {

			@Override
			public String map(Tuple1<String> value) throws Exception {
				return value.f0;
			}
		});
	}

	private static class Sum implements ReduceFunction<Double> {

		@Override
		public Double reduce(Double value1, Double value2) throws Exception {
			return value1 + value2;
		}

	}

	private static class ArrayToString implements MapFunction<Tuple2<String, Double>[], String> {

		@Override
		public String map(Tuple2<String, Double>[] value) throws Exception {
			return Arrays.toString(value);
		}
	}

	private static class Parser implements FlatMapFunction<String, Tuple2<String, String>> {

		@Override
		public void flatMap(String in, Collector<Tuple2<String, String>> out) throws Exception {
			if (in.equals("fail")) {
				throw new RuntimeException("Failed");
			}
			try {
				String s = in.replaceFirst(" ", ":").replace(" ", "");
				int splitPoint = s.indexOf(":");
				out.collect(Tuple2.of(s.substring(0, splitPoint), s.substring(splitPoint + 1)));
			} catch (Exception e) {
				System.err.println("Parsing error: " + in);
			}
		}
	}
}
