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

package streamkv.api.java.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static streamkv.api.java.operation.Operation.And;
import static streamkv.api.java.operation.Operation.Get;
import static streamkv.api.java.operation.Operation.Put;
import static streamkv.api.java.operation.Operation.Literal;
import static streamkv.api.java.operation.Operation.Update;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import streamkv.api.java.KVStore;
import streamkv.api.java.OperationOrdering;
import streamkv.api.java.Query;
import streamkv.api.java.operation.Operation;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationSerializer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TransactionalKVStoreOperatorTest implements Serializable {
	private static final long serialVersionUID = 1L;
	static Random rnd = new Random();

	@Test
	public void testKVOperator() throws Exception {

		AsyncKVStoreOperator<String, Integer> operator = new AsyncKVStoreOperator<>(
				new KVOperationSerializer<String, Integer>(StringSerializer.INSTANCE, IntSerializer.INSTANCE, null,
						null, null));

		OneInputStreamOperatorTestHarness<KVOperation<String, Integer>, KVOperation<String, Integer>> testHarness = new OneInputStreamOperatorTestHarness<>(
				operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1)));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "b", 2)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "b")));

		// Start PUT(GET()) transaction
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "a", 5, 1)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1)));

		assertEquals(operator.lockedKeys, ImmutableSet.of("a"));
		assertEquals(operator.runningTransactions, ImmutableMap.of(5L, ImmutableMap.of("a", 1)));

		// Process PUT(??)
		KVOperation<String, Integer> put = KVOperation.<String, Integer> put(0, "a", null).setOpID(2)
				.setTransactionID(5).setInputIDs(new long[] { 10 });
		testHarness.processElement(new StreamRecord<>(put));

		assertEquals(operator.runningOperations,
				ImmutableMap.of(2L, Tuple2.of(put, new ArrayList<KVOperation<String, Integer>>())));
		assertEquals(operator.inputMapping, ImmutableMap.of(10L, 2L));
		assertEquals(operator.lockedKeys, ImmutableSet.of("a"));
		assertEquals(operator.runningTransactions, ImmutableMap.of(5L, ImmutableMap.of("a", 1)));

		// Process x = GET() -> PUT(x)
		KVOperation<String, Integer> getRes = KVOperation.<String, Integer> kvRes(0, "b", 2).setDependentKey("a")
				.setOpID(10).setTransactionID(5).setInputIDs(new long[] {});
		testHarness.processElement(new StreamRecord<>(getRes));

		assertTrue(operator.lockedKeys.isEmpty());
		assertTrue(operator.runningTransactions.isEmpty());
		assertTrue(operator.runningOperations.isEmpty());
		assertTrue(operator.inputMapping.isEmpty());
		assertTrue(operator.inputs.isEmpty());

		// Start PUT(GET()) transaction
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "a", 5, 1)));
		assertEquals(operator.lockedKeys, ImmutableSet.of("a"));
		assertEquals(operator.runningTransactions, ImmutableMap.of(5L, ImmutableMap.of("a", 1)));

		// Process x = GET() -> PUT(x)
		KVOperation<String, Integer> getRes2 = KVOperation.<String, Integer> kvRes(0, "b", 2).setDependentKey("a")
				.setOpID(10).setTransactionID(5).setInputIDs(new long[] {});
		testHarness.processElement(new StreamRecord<>(getRes2));

		assertTrue(operator.runningOperations.isEmpty());
		assertTrue(operator.inputMapping.isEmpty());
		assertEquals(operator.inputs, ImmutableMap.of(10L, getRes2));
		assertEquals(operator.lockedKeys, ImmutableSet.of("a"));
		assertEquals(operator.runningTransactions, ImmutableMap.of(5L, ImmutableMap.of("a", 1)));

		// Process PUT(??)
		KVOperation<String, Integer> put2 = KVOperation.<String, Integer> put(0, "a", null).setOpID(2)
				.setTransactionID(5).setInputIDs(new long[] { 10 });
		testHarness.processElement(new StreamRecord<>(put2));

		assertTrue(operator.lockedKeys.isEmpty());
		assertTrue(operator.runningTransactions.isEmpty());
		assertTrue(operator.runningOperations.isEmpty());
		assertTrue(operator.inputMapping.isEmpty());
		assertTrue(operator.inputs.isEmpty());

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1)));

		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "d", 100)));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "c", 200)));

		// Transaction (999)
		// PUT("a", GET("b"))
		// UPDATE("d", GET("b"), +)
		// PUT("c", 10)

		// Create individual operations
		KVOperation<String, Integer> p = KVOperation.<String, Integer> put(0, "a", null).setOpID(0)
				.setTransactionID(999).setInputIDs(new long[] { 6 });

		KVOperation<String, Integer> g1 = KVOperation.<String, Integer> get(0, "b").setOpID(1).setTransactionID(999)
				.setInputIDs(new long[] {}).setDependentKey("d");
		KVOperation<String, Integer> g2 = KVOperation.<String, Integer> get(0, "b").setOpID(6).setTransactionID(999)
				.setInputIDs(new long[] {}).setDependentKey("a");

		KVOperation<String, Integer> gr1 = KVOperation.<String, Integer> kvRes(0, "b", 2).setDependentKey("d")
				.setOpID(1).setTransactionID(999).setInputIDs(new long[] {});

		KVOperation<String, Integer> gr2 = KVOperation.<String, Integer> kvRes(0, "b", 2).setOpID(6)
				.setTransactionID(999).setInputIDs(new long[] {}).setDependentKey("a");

		KVOperation<String, Integer> u = KVOperation.<String, Integer> update(0, "d", null).setOpID(2)
				.setTransactionID(999).setInputIDs(new long[] { 1 });
		u.reducer = AsyncKVStoreOperatorTest.sum;

		KVOperation<String, Integer> p2 = KVOperation.<String, Integer> put(0, "c", 10).setOpID(3)
				.setTransactionID(999).setInputIDs(new long[] {});

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "a", 999, 1)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "b", 999, 2)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "c", 999, 1)));

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "b")));

		testHarness.processElement(new StreamRecord<>(p));
		testHarness.processElement(new StreamRecord<>(g1));
		testHarness.processElement(new StreamRecord<>(g2));
		testHarness.processElement(new StreamRecord<>(gr1));
		testHarness.processElement(new StreamRecord<>(gr2));
		// get result before lock
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> lock(1, "d", 999, 1)));
		testHarness.processElement(new StreamRecord<>(u));
		testHarness.processElement(new StreamRecord<>(p2));

		assertTrue(operator.lockedKeys.isEmpty());
		assertTrue(operator.runningTransactions.isEmpty());
		assertTrue(operator.runningOperations.isEmpty());
		assertTrue(operator.inputMapping.isEmpty());
		assertTrue(operator.inputs.isEmpty());

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "d")));

		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "a", 1)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "b", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "a", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "a", 2)));
		expectedOutput.add(new StreamRecord<>(gr1));
		expectedOutput.add(new StreamRecord<>(gr2));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "b", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "a", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(0, "d", 102).setOpID(2).setTransactionID(999)
				.setInputIDs(new long[0])));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "c", 10)));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "d", 102)));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void integrationTest() throws Exception {
		System.out.println(Operation.createTransaction(2, Put(Literal(2), Get(Literal(3), null))));
		System.out.println("------");
		System.out.println(Operation.createTransaction(-1,
				Update(Literal(2), Update(Literal(3), Update(Literal(4), Get(Literal(5), null))))));
	}

	public void testDoubleUpdate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		final KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.ARRIVALTIME);

		List<Tuple2<String, Integer>> put = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			put.add(Tuple2.of(((Long) rnd.nextLong()).toString(), rnd.nextInt()));
		}

		List<Tuple2<String, String>> update = new ArrayList<>();
		for (int i = 0; i < 10000000; i++) {
			update.add(Tuple2.of(((Long) rnd.nextLong()).toString(), ((Long) rnd.nextLong()).toString()));
		}

		store.put(rebalance(env.fromCollection(put)));

		Query<?> u = store.applyOperation(rebalance(env.fromCollection(update)).map(
				new MapFunction<Tuple2<String, String>, Operation<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Operation<String, Integer> map(Tuple2<String, String> t) throws Exception {
						return And(Update(Literal(t.f0), Get(Literal(t.f1), store)),
								Update(Literal(t.f1), Get(Literal(t.f0), store)));
					}
				}));

		store.setReducerForQuery(u.getID(), new ReduceFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer g1, Integer g2) throws Exception {
				if (g1 == null || g2 == null) {
					return 1;
				} else {
					return Math.min(g1, g2);

				}
			}
		});

		store.get(env.fromCollection(put).map(new MapFunction<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map(Tuple2<String, Integer> t) throws Exception {
				Thread.sleep(1);
				return t.f0;
			}
		})).getOutput();

		JobExecutionResult r = env.execute();
		System.out.println(r.getNetRuntime());

	}

	private <T> DataStream<T> rebalance(DataStream<T> in) {
		return in.global().filter(new FilterFunction<T>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(T arg0) throws Exception {
				return true;
			}
		});
	}
}
