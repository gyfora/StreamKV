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

package streamkv.api.java.operator.checkpointing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import streamkv.api.java.KVStore;
import streamkv.api.java.OperationOrdering;
import streamkv.api.java.Query;

public class KVStoreCheckpointingTest {

	final static int NUM_KEYS = 25;
	static List<Tuple2<String, Integer>> input;
	static List<Tuple2<String, Integer>[]> mgetInput;
	protected static final int NUM_TASK_MANAGERS = 2;
	protected static final int NUM_TASK_SLOTS = 3;
	protected static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static Random rnd = new Random();

	private static ForkableFlinkMiniCluster cluster;

	public void testProgram(StreamExecutionEnvironment env) {

		env.getConfig().enableTimestamps();

		input = generateInputs(env.getParallelism() * 10000);
		mgetInput = generateKVArrays(input);

		DataStream<Tuple2<String, Integer>> putSource = env.addSource(new PutSource(input));
		DataStream<String> getSource = env.addSource(new GetSource(input));
		DataStream<String[]> mGetSource = env.addSource(new MGetSource(toMget(mgetInput)));

		KVStore<String, Integer> store = KVStore.withOrdering(OperationOrdering.TIMESTAMP);

		store.put(putSource);

		Query<Tuple2<String, Integer>> getQ = store.get(getSource);
		Query<Tuple2<String, Integer>[]> mgetQ = store.multiGet(mGetSource);

		getQ.getOutput().map(new OnceFailingMapper()).addSink(new CollectingSink<Tuple2<String, Integer>>());

		mgetQ.getOutput().addSink(new CollectingSink2<Tuple2<String, Integer>[]>());
	}

	public void postSubmit() {
		assertTrue(OnceFailingMapper.failed);
		assertEquals(new HashSet<>(input), CollectingSink.collected);
		assertEquals(toSetOfSets(mgetInput), toSetOfSets(CollectingSink2.collected));

	}

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);

			cluster = new ForkableFlinkMiniCluster(config, false);

			cluster.start();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void stopCluster() {
		try {
			cluster.stop();
			cluster = null;
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}

	@Test
	public void runCheckpointedProgram() {
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost",
					cluster.getLeaderRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(1000);
			env.getConfig().disableSysoutLogging();

			testProgram(env);

			env.execute();

			postSubmit();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static class CollectingSink<T> implements SinkFunction<T> {
		private static final long serialVersionUID = 1L;

		public static Set<Object> collected = Collections
				.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());

		@Override
		public void invoke(T value) throws Exception {
			collected.add(value);
		}

	}

	private static class CollectingSink2<T> implements SinkFunction<T> {
		private static final long serialVersionUID = 1L;

		public static Set<Object> collected = Collections
				.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());

		@Override
		public void invoke(T value) throws Exception {
			collected.add(value);
		}

	}

	private static class OnceFailingMapper implements
			MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;
		private static volatile boolean failed = false;
		private int c = 0;

		@Override
		public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
			if (!failed && c++ == 5000) {
				failed = true;
				throw new RuntimeException("FAIL");
			} else {
				return value;
			}
		}

	}

	private List<Tuple2<String, Integer>> generateInputs(int len) {
		List<Tuple2<String, Integer>> output = new ArrayList<>();
		Random rnd = new Random();

		for (int i = 0; i < len; i++) {
			output.add(Tuple2.of("" + rnd.nextInt(NUM_KEYS), rnd.nextInt()));
		}

		return output;
	}

	@SuppressWarnings("unchecked")
	private List<Tuple2<String, Integer>[]> generateKVArrays(List<Tuple2<String, Integer>> inputs) {
		List<Tuple2<String, Integer>[]> out = new ArrayList<>();

		for (int i = 0; i < inputs.size(); i++) {
			int nk = rnd.nextInt(i < 10 ? i + 1 : 10) + 1;
			List<Tuple2<String, Integer>> kvs = new ArrayList<>();
			Set<String> keys = new HashSet<>();
			for (int j = 0; j < nk; j++) {
				Tuple2<String, Integer> candidate = inputs.get(i - j);
				if (!keys.contains(candidate.f0)) {
					kvs.add(candidate);
					keys.add(candidate.f0);
				}
			}
			out.add(kvs.toArray(new Tuple2[kvs.size()]));
		}

		return out;
	}

	private List<String[]> toMget(List<Tuple2<String, Integer>[]> input) {
		List<String[]> out = new ArrayList<>();
		for (Tuple2<String, Integer>[] arr : input) {
			String[] outArr = new String[arr.length];
			for (int i = 0; i < arr.length; i++) {
				outArr[i] = arr[i].f0;
			}
			out.add(outArr);
		}
		return out;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<Set<Tuple2<String, Integer>>> toSetOfSets(Collection c) {
		Set<Set<Tuple2<String, Integer>>> out = new HashSet<>();
		for (Object o : c) {
			Set<Tuple2<String, Integer>> kvSet = new HashSet<>();
			for (Tuple2<String, Integer> t : (Tuple2<String, Integer>[]) o) {
				kvSet.add(t);
			}
			out.add(kvSet);
		}
		return out;
	}

	public static class PutSource extends RichParallelSourceFunction<Tuple2<String, Integer>> implements
			EventTimeSourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private List<Tuple2<String, Integer>> inputs;
		private OperatorState<Integer> offset;

		private volatile boolean isRunning = false;

		public PutSource(List<Tuple2<String, Integer>> inputs) {
			this.inputs = inputs;
		}

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			isRunning = true;
			synchronized (ctx.getCheckpointLock()) {
				while (isRunning && offset.value() < inputs.size()) {
					Tuple2<String, Integer> input = inputs.get(offset.value());
					long time = 2 * offset.value();
					ctx.collectWithTimestamp(Tuple2.of(input.f0, input.f1), time);
					if (rnd.nextDouble() < 0.1) {
						ctx.emitWatermark(new Watermark(time));

					}
					offset.update(offset.value() + getRuntimeContext().getNumberOfParallelSubtasks());
					if (rnd.nextDouble() < 0.001) {
						Thread.sleep(10);
					}
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void open(Configuration c) throws IOException {
			offset = getRuntimeContext().getOperatorState("offset",
					getRuntimeContext().getIndexOfThisSubtask(), false);
		}

	}

	public static class GetSource extends RichParallelSourceFunction<String> implements
			EventTimeSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private List<Tuple2<String, Integer>> inputs;
		private OperatorState<Integer> offset;

		private volatile boolean isRunning = false;

		public GetSource(List<Tuple2<String, Integer>> inputs) {
			this.inputs = inputs;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			isRunning = true;
			synchronized (ctx.getCheckpointLock()) {
				while (isRunning && offset.value() < inputs.size()) {
					Tuple2<String, Integer> input = inputs.get(offset.value());
					long time = 2 * offset.value() + 1;
					ctx.collectWithTimestamp(input.f0, time);
					if (rnd.nextDouble() < 0.1) {
						ctx.emitWatermark(new Watermark(time));

					}
					offset.update(offset.value() + getRuntimeContext().getNumberOfParallelSubtasks());
					if (rnd.nextDouble() < 0.001) {
						Thread.sleep(10);
					}
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void open(Configuration c) throws IOException {
			offset = getRuntimeContext().getOperatorState("offset",
					getRuntimeContext().getIndexOfThisSubtask(), false);
		}

	}

	public static class MGetSource extends RichParallelSourceFunction<String[]> implements
			EventTimeSourceFunction<String[]> {
		private static final long serialVersionUID = 1L;

		private List<String[]> inputs;
		private OperatorState<Integer> offset;

		private volatile boolean isRunning = false;

		public MGetSource(List<String[]> inputs) {
			this.inputs = inputs;
		}

		@Override
		public void run(SourceContext<String[]> ctx) throws Exception {
			isRunning = true;
			synchronized (ctx.getCheckpointLock()) {
				while (isRunning && offset.value() < inputs.size()) {
					long time = 2 * offset.value() + 1;
					ctx.collectWithTimestamp(inputs.get(offset.value()), time);
					if (rnd.nextDouble() < 0.1) {
						ctx.emitWatermark(new Watermark(time));
					}
					offset.update(offset.value() + getRuntimeContext().getNumberOfParallelSubtasks());
					if (rnd.nextDouble() < 0.001) {
						Thread.sleep(10);
					}
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void open(Configuration c) throws IOException {
			offset = getRuntimeContext().getOperatorState("offset",
					getRuntimeContext().getIndexOfThisSubtask(), false);
		}

	}

}
