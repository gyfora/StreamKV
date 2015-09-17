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

package streamkv.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import streamkv.api.KVStore.OperationOrdering;

public class TSKVStoreIntegrationTest implements Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Test
	public void integrationTest() throws Exception {

		// Fetch the environment and enable timestamps
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.getConfig().disableSysoutLogging();
		env.getConfig().enableTimestamps();

		// Define query input streams 
		DataStream<Tuple2<String, Integer>> put1 = env.addSource(
				new PutSource(
						Tuple3.of("a", 1, 1L), 
						Tuple3.of("b", 2, 5L)));

		DataStream<Tuple2<String, Integer>> put2 = env.addSource(
				new PutSource(
						Tuple3.of("a", -1, 3L), 
						Tuple3.of("b", -1, 8L), 
						Tuple3.of("c", 0, 12L)));

		DataStream<Tuple2<String, Long>> sget = env.addSource(
				new SGetSource(
						Tuple2.of("a", 2L), 
						Tuple2.of("a", 5L), 
						Tuple2.of("b", 9L), 
						Tuple2.of("c", 11L)));

		DataStream<Tuple2<String, Long>[]> smget = env.addSource(
				new SMGetSource(
						arrayOf(
								Tuple2.of("a", 2L), 
								Tuple2.of("b", 2L)), 
						arrayOf(
								Tuple2.of("b", 13L), 
								Tuple2.of("b", 13L), 
								Tuple2.of("c", 13L)))).returns("Tuple2<String, Long>[]");

		DataStream<String> get = env.addSource(
				new GetSource(
						Tuple2.of("a", 0L), 
						Tuple2.of("b", 7L), 
						Tuple2.of("a", 8L), 
						Tuple2.of("c", 10L), 
						Tuple2.of("c", 13L)));

		// Create KVStore and apply operations
		KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.TIME);

		store.put(put1);
		store.put(put2);

		int id1 = store.getWithKeySelector(sget, new MySelector());
		int id2 = store.multiGetWithKeySelector(smget, new MySelector());
		int id3 = store.get(get);

		// Get and collect the outputs
		KVStoreOutput<String, Integer> storeOutputs = store.getOutputs();
		
		storeOutputs.<Tuple2<String, Long>> getKVStream(id1).addSink(new CollectingSink1<Tuple2<Tuple2<String, Long>, Integer>>());
		storeOutputs.<Tuple2<String, Long>> getKVArrayStream(id2).addSink(new CollectingSink2<Tuple2<Tuple2<String, Long>, Integer>[]>());
		storeOutputs.<String> getKVStream(id3).addSink(new CollectingSink3<Tuple2<String, Integer>>());

		env.execute();

		// Define the expected output
		Set<Tuple2<Tuple2<String, Long>, Integer>> sgetExpectedOutput = new HashSet<>();
		List<Tuple2<Tuple2<String, Long>, Integer>[]> smgetExpectedOutput = new ArrayList<>();
		Set<Tuple2<String, Integer>> getExpectedOutput = new HashSet<>();

		sgetExpectedOutput.add(Tuple2.of(Tuple2.of("a", 2L), 1));
		sgetExpectedOutput.add(Tuple2.of(Tuple2.of("a", 5L), -1));
		sgetExpectedOutput.add(Tuple2.of(Tuple2.of("b", 9L), -1));
		sgetExpectedOutput.add(Tuple2.of(Tuple2.of("c", 11L), (Integer) null));

		smgetExpectedOutput.add(
				arrayOf(
						Tuple2.of(Tuple2.of("a", 2L), 1),
						Tuple2.of(Tuple2.of("b", 2L), (Integer) null)));
		smgetExpectedOutput.add(
				arrayOf(
						Tuple2.of(Tuple2.of("b", 13L), -1),
						Tuple2.of(Tuple2.of("b", 13L), -1), 
						Tuple2.of(Tuple2.of("c", 13L), 0)));

		getExpectedOutput.add(Tuple2.of("a", (Integer) null));
		getExpectedOutput.add(Tuple2.of("b", 2));
		getExpectedOutput.add(Tuple2.of("a", -1));
		getExpectedOutput.add(Tuple2.of("c", (Integer) null));
		getExpectedOutput.add(Tuple2.of("c", 0));

		// Validate outputs
		assertEquals(sgetExpectedOutput, CollectingSink1.collected);
		validateSelectorMultigetOutput(smgetExpectedOutput);
		assertEquals(getExpectedOutput, CollectingSink3.collected);
	}

	@SuppressWarnings("unchecked")
	private void validateSelectorMultigetOutput(List<Tuple2<Tuple2<String, Long>, Integer>[]> expected) {
		// Sort output by timestamps
		List<Tuple2<Tuple2<String, Long>, Integer>[]> actual = CollectingSink2.collected;
		Collections.sort(actual, new Comparator<Tuple2<Tuple2<String, Long>, Integer>[]>() {

			@Override
			public int compare(Tuple2<Tuple2<String, Long>, Integer>[] o1,
					Tuple2<Tuple2<String, Long>, Integer>[] o2) {
				return o1[0].f0.f1.compareTo(o2[0].f0.f1);
			}

		});

		// Sort output arrays by key
		for (Tuple2<Tuple2<String, Long>, Integer>[] arr : actual) {
			Arrays.sort(arr, new Comparator<Tuple2<Tuple2<String, Long>, Integer>>() {

				@Override
				public int compare(Tuple2<Tuple2<String, Long>, Integer> o1,
						Tuple2<Tuple2<String, Long>, Integer> o2) {
					return o1.f0.f0.compareTo(o2.f0.f0);
				}

			});
		}

		assertEquals(expected.size(), actual.size());

		Iterator<Tuple2<Tuple2<String, Long>, Integer>[]> expectedIT = expected.iterator();
		Iterator<Tuple2<Tuple2<String, Long>, Integer>[]> actualIT = actual.iterator();

		while (expectedIT.hasNext()) {
			assertArrayEquals(expectedIT.next(), actualIT.next());
		}
	}
	
	public static class MySelector implements KeySelector<Tuple2<String, Long>, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Long> value) throws Exception {
			return value.f0;
		}
	}

	public static class CollectingSink1<T> implements SinkFunction<T> {
		private static final long serialVersionUID = 1L;

		public static Set<Object> collected = Collections
				.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());

		@Override
		public void invoke(T value) throws Exception {
			collected.add(value);
		}

	}

	public static class CollectingSink2<T> implements SinkFunction<T> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("rawtypes")
		public static List collected = Collections.synchronizedList(new ArrayList<>());

		@SuppressWarnings("unchecked")
		@Override
		public void invoke(T value) throws Exception {
			collected.add(value);
		}

	}

	public static class CollectingSink3<T> implements SinkFunction<T> {
		private static final long serialVersionUID = 1L;

		public static Set<Object> collected = Collections
				.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());

		@Override
		public void invoke(T value) throws Exception {
			collected.add(value);
		}

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
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (Tuple3<String, Integer, Long> input : inputs) {
					ctx.collectWithTimestamp(Tuple2.of(input.f0, input.f1), input.f2);
					ctx.emitWatermark(new Watermark(input.f2));
				}
			}
		}

		@Override
		public void cancel() {
		}

	}

	public static class SGetSource implements SourceFunction<Tuple2<String, Long>>,
			EventTimeSourceFunction<Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		Tuple2<String, Long>[] inputs;

		@SafeVarargs
		public SGetSource(Tuple2<String, Long>... input) {
			this.inputs = input;
		}

		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
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

	public static class GetSource implements SourceFunction<String>, EventTimeSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		Tuple2<String, Long>[] inputs;

		@SafeVarargs
		public GetSource(Tuple2<String, Long>... input) {
			this.inputs = input;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (Tuple2<String, Long> input : inputs) {
					ctx.collectWithTimestamp(input.f0, input.f1);
					ctx.emitWatermark(new Watermark(input.f1));
				}
			}

		}

		@Override
		public void cancel() {
		}

	}

	public static class SMGetSource implements SourceFunction<Tuple2<String, Long>[]>,
			EventTimeSourceFunction<Tuple2<String, Long>[]> {
		private static final long serialVersionUID = 1L;

		Tuple2<String, Long>[][] inputs;

		@SafeVarargs
		public SMGetSource(Tuple2<String, Long>[]... input) {
			this.inputs = input;
		}

		@Override
		public void run(SourceContext<Tuple2<String, Long>[]> ctx)
				throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (Tuple2<String, Long>[] input : inputs) {
					ctx.collectWithTimestamp(input, input[0].f1);
					ctx.emitWatermark(new Watermark(input[0].f1));
				}
			}

		}

		@Override
		public void cancel() {
		}

	}

	private <X> X[] arrayOf(@SuppressWarnings("unchecked") X... in) {
		return in;
	}

}
