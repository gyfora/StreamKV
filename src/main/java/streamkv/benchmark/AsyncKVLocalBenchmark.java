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

package streamkv.benchmark;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import streamkv.api.AsyncKVStore;
import streamkv.api.KVStore;

public class AsyncKVLocalBenchmark {
	
	public static final long ELEMENTS_PER_QUERY = 50_000_000L;
	public static final int NUM_KEYS = 1000;


	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);

		KVStore<Integer, Integer> store = new AsyncKVStore<>();

		DataStream<Tuple2<Integer, Integer>> putStream = env.addSource(new PutGenerator(ELEMENTS_PER_QUERY, NUM_KEYS));
		DataStream<Integer> getStream = env.addSource(new GetGenerator(ELEMENTS_PER_QUERY, NUM_KEYS));

		store.put(putStream);
		int getId = store.get(getStream);

		store.getOutputs().getKVStream(getId).addSink(new NoOpSink<Tuple2<Integer, Integer>>());

		JobExecutionResult result = env.execute();

		System.out.println("Net runtime: " + result.getNetRuntime(TimeUnit.SECONDS) + "s");

	}

	public static class NoOpSink<T> implements SinkFunction<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(T value) throws Exception {
		}

	}

	public static class PutGenerator extends RichParallelSourceFunction<Tuple2<Integer, Integer>> {
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
			long maxCount = numElements / getRuntimeContext().getNumberOfParallelSubtasks();
			long c = 0;
			Tuple2<Integer, Integer> reuse = new Tuple2<>();

			while (isRunning && c++ < maxCount) {
				reuse.f0 = rnd.nextInt(numKeys);
				reuse.f1 = rnd.nextInt();
				ctx.collect(reuse);
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static class GetGenerator extends RichParallelSourceFunction<Integer> {
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
			long maxCount = numElements / getRuntimeContext().getNumberOfParallelSubtasks();
			long c = 0;

			while (isRunning && c++ < maxCount) {
				ctx.collect(rnd.nextInt(numKeys));
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

}
