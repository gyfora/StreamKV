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

package streamkv.api.java;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * This test verifies that the stream graph is created without errors and types
 * are inferred properly for the output transformations.
 * 
 */
@SuppressWarnings("serial")
public class KVStoreApiTest implements Serializable {

	@Test
	public void test() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KVStore<String, MyPojo> kvStore = KVStore.withOrdering(OperationOrdering.ARRIVALTIME);

		kvStore.update(env.fromElements(Tuple2.of("a", new MyPojo())), new ReduceFunction<MyPojo>() {

			@Override
			public MyPojo reduce(MyPojo value1, MyPojo value2) throws Exception {
				return null;
			}
		});

		Query<Tuple2<String, MyPojo>> getQ = kvStore.get(env.fromElements("a"));
		Query<Tuple2<String, MyPojo>> removeQ = kvStore.remove(env.fromElements("a"));
		Query<Tuple2<Integer, MyPojo>> sGetQ = kvStore.getWithKeySelector(env.fromElements(1),
				new KeySelector<Integer, String>() {

					@Override
					public String getKey(Integer value) throws Exception {
						return value.toString();
					}
				});
		Query<Tuple2<String, MyPojo>[]> mGetQ = kvStore.multiGet(env.fromElements(new String[] { "a" },
				new String[] { "a" }));

		Query<Tuple2<Integer, MyPojo>[]> sMGetQ = kvStore.multiGetWithKeySelector(
				env.fromElements(new Integer[] { 1 }, new Integer[] { 2 }), new KeySelector<Integer, String>() {

					@Override
					public String getKey(Integer value) throws Exception {
						return value.toString();
					}
				});

		assertEquals(6, kvStore.getQueries().size());

		getQ.getOutput().keyBy(0).map(new MapFunction<Tuple2<String, MyPojo>, MyPojo>() {

			@Override
			public MyPojo map(Tuple2<String, MyPojo> value) throws Exception {
				return value.f1;
			}
		}).shuffle().print();

		removeQ.getOutput().project(0).print();

		sGetQ.getOutput().keyBy("f0").flatMap(new FlatMapFunction<Tuple2<Integer, MyPojo>, String>() {

			@Override
			public void flatMap(Tuple2<Integer, MyPojo> value, Collector<String> out) throws Exception {
			}
		}).filter(new FilterFunction<String>() {

			@Override
			public boolean filter(String value) throws Exception {
				return false;
			}
		});

		mGetQ.getOutput().map(new MapFunction<Tuple2<String, MyPojo>[], Tuple2<String, MyPojo>>() {

			@Override
			public Tuple2<String, MyPojo> map(Tuple2<String, MyPojo>[] value) throws Exception {
				return value[0];
			}
		}).print();

		sMGetQ.getOutput().map(new MapFunction<Tuple2<Integer, MyPojo>[], Tuple2<String, MyPojo>>() {

			@Override
			public Tuple2<String, MyPojo> map(Tuple2<Integer, MyPojo>[] value) throws Exception {
				return null;
			}
		}).print();

		env.getStreamGraph().getJobGraph();

	}

	@SuppressWarnings("unused")
	private class MyPojo {
		public Tuple2<String, MyPojo> f0;
		public String f1;

		public MyPojo(Tuple2<String, MyPojo> f0, String f1) {
			super();
			this.f0 = f0;
			this.f1 = f1;
		}

		public MyPojo() {

		}
	}

}
