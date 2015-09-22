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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import streamkv.api.java.KVStore;

public abstract class LocalBenchmark implements Serializable {

	private static final long serialVersionUID = 1L;

	protected int CHECKPOINT_INTERVAL = 1000;
	protected String prefix = "";

	protected abstract SourceFunction<Integer> getSource();

	protected abstract SourceFunction<Tuple2<Integer, Integer>> putSource();

	protected abstract SourceFunction<Integer[]> multiGetSource();

	protected abstract KVStore<Integer, Integer> getStore();

	public void benchmarkGet() throws Exception {
		KVStore<Integer, Integer> store = getStore();
		StreamExecutionEnvironment env = setup(store);

		DataStream<Integer> getStream = env.addSource(getSource());

		store.get(getStream);

		runBenchmark(env, store, "Get");
	}

	public void benchmarkMultiget() throws Exception {

		KVStore<Integer, Integer> store = getStore();
		StreamExecutionEnvironment env = setup(store);

		DataStream<Integer[]> multiGetStream = env.addSource(multiGetSource());

		store.multiGet(multiGetStream);
		runBenchmark(env, store, "MultiGet");
	}

	public void benchmarkSelectorGet() throws Exception {
		KVStore<Integer, Integer> store = getStore();
		StreamExecutionEnvironment env = setup(store);

		DataStream<String> getStream = env.addSource(getSource()).map(new MapFunction<Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map(Integer value) throws Exception {
				return value.toString();
			}
		});

		store.getWithKeySelector(getStream, new KeySelector<String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(String value) throws Exception {
				return Integer.valueOf(value);
			}
		});

		runBenchmark(env, store, "SelectorGet");
	}

	public StreamExecutionEnvironment setup(KVStore<Integer, Integer> store) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(CHECKPOINT_INTERVAL);
		setupEnv(env);
		DataStream<Tuple2<Integer, Integer>> putStream = env.addSource(putSource());

		store.put(putStream);
		return env;
	}

	protected void setupEnv(StreamExecutionEnvironment env) {

	}

	public void runBenchmark(StreamExecutionEnvironment env, KVStore<Integer, Integer> store,
			String description) throws Exception {
		store.getOutputs();

		JobExecutionResult result = env.execute();

		System.err.println("Net runtime (" + prefix + description + "): "
				+ result.getNetRuntime(TimeUnit.SECONDS) + "s");
	}

}
