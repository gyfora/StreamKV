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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import streamkv.api.KVStore;
import streamkv.api.KVStore.OperationOrdering;
import streamkv.api.KVStoreOutput;

public class TSKVStreamExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().enableTimestamps();
		// Create a new Key-Value store
		KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.TIME);

		DataStream<Tuple2<String, Integer>> put1 = env.addSource(new PutSource(
				Tuple3.of("a", 1, 1L),
				Tuple3.of("a", 2, 5L), 
				Tuple3.of("b", 1, 6L), 
				Tuple3.of("a", 3, 7L),
				Tuple3.of("c", 1, 8L), 
				Tuple3.of("c", 2, 10L)));
		DataStream<Tuple2<String, Integer>> put2 = env.addSource(new PutSource(
				Tuple3.of("a", -1, 3L),
				Tuple3.of("b", -1, 8L), 
				Tuple3.of("c", -1, 12L)));

		DataStream<Tuple2<String, Long>> get = env.addSource(new GetSource(
				Tuple2.of("a",2L),
				Tuple2.of("a",4L),
				Tuple2.of("b",5L),
				Tuple2.of("a",6L),
				Tuple2.of("b",7L),
				Tuple2.of("a",8L),
				Tuple2.of("b",9L),
				Tuple2.of("c",9L),
				Tuple2.of("c",11L),
				Tuple2.of("c",13L)
				));

		store.put(put1);
		store.put(put2);

		int id1 = store.getWithKeySelector(get, new KeySelector<Tuple2<String, Long>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple2<String, Long> value) throws Exception {
				return value.f0;
			}
		});

		KVStoreOutput<String, Integer> storeOutputs = store.getOutputs();
		storeOutputs.getKVStream(id1).print();

		env.execute();
	}

	public static class PutSource implements SourceFunction<Tuple2<String, Integer>>,
			EventTimeSourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		Tuple3<String, Integer, Long>[] inputs;

		@SafeVarargs
		public PutSource(Tuple3<String, Integer, Long>... input) {
			this.inputs = input;
		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Integer>> ctx)
				throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (Tuple3<String, Integer, Long> input : inputs) {
					ctx.collectWithTimestamp(Tuple2.of(input.f0,input.f1), input.f2);
					ctx.emitWatermark(new Watermark(input.f2));
				}
			}
		}

		@Override
		public void cancel() {
		}

	}

	public static class GetSource implements SourceFunction<Tuple2<String, Long>>,
			EventTimeSourceFunction<Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		Tuple2<String, Long>[] inputs;

		@SafeVarargs
		public GetSource(Tuple2<String, Long>... input) {
			this.inputs = input;
		}

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<String, Long>> ctx)
				throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (Tuple2<String, Long> input : inputs) {
					ctx.collectWithTimestamp(input, input.f1);
					ctx.emitWatermark(new Watermark(input.f1));
				}
			}

		}

		@Override
		public void cancel() {
		}

	}
}
