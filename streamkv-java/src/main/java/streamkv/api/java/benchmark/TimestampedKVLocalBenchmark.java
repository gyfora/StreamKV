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

package streamkv.api.java.benchmark;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import streamkv.api.java.KVStore;
import streamkv.api.java.OperationOrdering;

public class TimestampedKVLocalBenchmark extends LocalBenchmark {

	private static final long serialVersionUID = 1L;
	
	private static final long ELEMENTS_PER_QUERY = 1_000_000L;
	private static final int NUM_KEYS = 1000;
	private static final int KEYS_PER_MGET = 10;
	private static final int WATERMARK_FREQUENCY = 1000;

	public static void main(String[] args) throws Exception {
		LocalBenchmark bm = new TimestampedKVLocalBenchmark();
		bm.prefix = "Timestamped";
		bm.benchmarkGet();
		bm.benchmarkSelectorGet();
		bm.benchmarkMultiget();
	}

	@Override
	protected SourceFunction<Integer> getSource() {
		return new GetGenerator(ELEMENTS_PER_QUERY, NUM_KEYS);
	}

	@Override
	protected SourceFunction<Tuple2<Integer, Integer>> putSource() {
		return new PutGenerator(ELEMENTS_PER_QUERY, NUM_KEYS);
	}

	@Override
	protected SourceFunction<Integer[]> multiGetSource() {
		return new MultiGetGenerator(ELEMENTS_PER_QUERY, NUM_KEYS, KEYS_PER_MGET);
	}

	@Override
	protected KVStore<Integer, Integer> getStore() {
		return KVStore.withOrdering(OperationOrdering.TIMESTAMP);
	}

	@Override
	protected void setupEnv(StreamExecutionEnvironment env) {
		env.getConfig().enableTimestamps();
	}

	public static class PutGenerator extends RichParallelSourceFunction<Tuple2<Integer, Integer>> implements
			EventTimeSourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;
		private volatile boolean isRunning = false;
		private long numElements;
		private int numKeys;

		public PutGenerator(long numElements, int numKeys) {
			this.numElements = numElements;
			this.numKeys = numKeys;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			isRunning = true;

			Random rnd = new Random();
			long time = 0;
			long maxCount = numElements / getRuntimeContext().getNumberOfParallelSubtasks();
			long c = 0;
			Tuple2<Integer, Integer> reuse = new Tuple2<>();

			while (isRunning && c++ < maxCount) {
				reuse.f0 = rnd.nextInt(numKeys);
				reuse.f1 = rnd.nextInt();
				ctx.collectWithTimestamp(reuse, time++);
				if (c % WATERMARK_FREQUENCY == 0) {
					ctx.emitWatermark(new Watermark(time));
					Thread.sleep(1);
				}
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static class GetGenerator extends RichParallelSourceFunction<Integer> implements
			EventTimeSourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private volatile boolean isRunning = false;
		private long numElements;
		private int numKeys;

		public GetGenerator(long numElements, int numKeys) {
			this.numElements = numElements;
			this.numKeys = numKeys;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			isRunning = true;

			Random rnd = new Random();
			long time = 0;
			long maxCount = numElements / getRuntimeContext().getNumberOfParallelSubtasks();
			long c = 0;

			while (isRunning && c++ < maxCount) {
				ctx.collectWithTimestamp(rnd.nextInt(numKeys), c++);
				if (c % WATERMARK_FREQUENCY == 0) {
					ctx.emitWatermark(new Watermark(time));
					Thread.sleep(1);
				}
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static class MultiGetGenerator extends RichParallelSourceFunction<Integer[]> implements
			EventTimeSourceFunction<Integer[]> {
		private static final long serialVersionUID = 1L;
		private volatile boolean isRunning = false;
		private long numElements;
		private int numKeys;
		private int keysPerGet;

		public MultiGetGenerator(long numElements, int numKeys, int keysPerGet) {
			this.numElements = numElements;
			this.numKeys = numKeys;
			this.keysPerGet = keysPerGet;
		}

		@Override
		public void run(SourceContext<Integer[]> ctx) throws Exception {
			isRunning = true;

			Random rnd = new Random();
			long time = 0;
			long maxCount = numElements / getRuntimeContext().getNumberOfParallelSubtasks();
			long c = 0;

			while (isRunning && c++ < maxCount) {
				Integer[] keys = new Integer[rnd.nextInt(keysPerGet) + 1];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = rnd.nextInt(numKeys);
				}
				ctx.collectWithTimestamp(keys, time++);
				if (c % WATERMARK_FREQUENCY == 0) {
					ctx.emitWatermark(new Watermark(time));
				}
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

}
