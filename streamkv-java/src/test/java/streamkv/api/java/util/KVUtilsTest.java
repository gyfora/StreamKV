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

package streamkv.api.java.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.util.Collector;
import org.junit.Test;

import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationTypeInfo;
import streamkv.api.java.util.KVUtils;

import com.google.common.collect.ImmutableMap;

public class KVUtilsTest {

	@Test
	public void toPutTest() throws Exception {
		RichMapFunction<Tuple2<Integer, String>, KVOperation<Integer, String>> toPut = new KVUtils.ToPut<>(3);
		toPut.open(null);

		assertEquals(KVOperation.put(3, 2, "a"), toPut.map(Tuple2.of(2, "a")));
		assertEquals(KVOperation.<Integer, String> put(3, 1, "b"), toPut.map(Tuple2.of(1, "b")));
		try {
			toPut.map(Tuple2.of((Integer) null, "a"));
			fail();
		} catch (Exception e) {
			// good
		}
		try {
			toPut.map(Tuple2.of(2, (String) null));
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toUpdateTest() throws Exception {
		RichMapFunction<Tuple2<Integer, String>, KVOperation<Integer, String>> toUpdate = new KVUtils.ToUpdate<>(
				3);
		toUpdate.open(null);

		assertEquals(KVOperation.update(3, 2, "a"), toUpdate.map(Tuple2.of(2, "a")));
		assertEquals(KVOperation.<Integer, String> update(3, 1, "b"), toUpdate.map(Tuple2.of(1, "b")));
		try {
			toUpdate.map(Tuple2.of((Integer) null, "a"));
			fail();
		} catch (Exception e) {
			// good
		}
		try {
			toUpdate.map(Tuple2.of(1, (String) null));
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toGetTest() throws Exception {
		RichMapFunction<Integer, KVOperation<Integer, String>> toGet = new KVUtils.ToGet<>(2);
		toGet.open(null);

		assertEquals(KVOperation.get(2, 2), toGet.map(2));
		try {
			toGet.map((Integer) null);
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toRemoveTest() throws Exception {
		RichMapFunction<Integer, KVOperation<Integer, String>> toRemove = new KVUtils.ToRemove<>(2);
		toRemove.open(null);

		assertEquals(KVOperation.remove(2, 2), toRemove.map(2));
		try {
			toRemove.map((Integer) null);
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toSGetTest() throws Exception {
		RichMapFunction<Object, KVOperation<Integer, String>> toSGet = new KVUtils.ToSGet<>(2);
		toSGet.open(null);

		assertEquals(KVOperation.selectorGet(2, "a"), toSGet.map("a"));
		try {
			toSGet.map((String) null);
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toMGetTest() throws Exception {
		RichFlatMapFunction<Integer[], KVOperation<Integer, String>> toMGet = new KVUtils.ToMGet<>(2);
		toMGet.open(null);

		MyCollector out = new MyCollector();

		toMGet.flatMap(new Integer[] { 1, 2, 3 }, out);

		long opID = out.elements.get(0).getOperationID();

		assertEquals(
				out.elements,
				Arrays.asList(KVOperation.multiGet(2, 1, (short) 3, opID),
						KVOperation.multiGet(2, 2, (short) 3, opID),
						KVOperation.multiGet(2, 3, (short) 3, opID)));

		try {
			toMGet.flatMap(new Integer[] { 1, null, 3 }, out);
			fail();
		} catch (Exception e) {
			// good
		}
		try {
			toMGet.flatMap(new Integer[] {}, out);
			fail();
		} catch (Exception e) {
			// good
		}
	}

	@Test
	public void toSMGetTest() throws Exception {
		RichFlatMapFunction<Object, KVOperation<Integer, String>> toSMGet = new KVUtils.ToSMGet<Integer, String>(
				2);
		toSMGet.open(null);

		MyCollector out = new MyCollector();

		toSMGet.flatMap(new String[] { "a", "b" }, out);

		long opID = out.elements.get(0).getOperationID();

		assertEquals(
				out.elements,
				Arrays.asList(KVOperation.selectorMultiGet(2, "a", (short) 2, opID),
						KVOperation.selectorMultiGet(2, "b", (short) 2, opID)));
		try {
			toSMGet.flatMap(new Integer[] { 1, null, 3 }, out);
			fail();
		} catch (Exception e) {
			// good
		}
		try {
			toSMGet.flatMap(new Integer[] {}, out);
			fail();
		} catch (Exception e) {
			// good
		}
	}

	private class MyCollector implements Collector<KVOperation<Integer, String>> {

		List<KVOperation<Integer, String>> elements = new ArrayList<>();
		@SuppressWarnings("rawtypes")
		TypeSerializer<KVOperation<Integer, String>> s = new KVOperationTypeInfo.KVOpSerializer<Integer, String>(
				IntSerializer.INSTANCE, null, null, ImmutableMap.of((short) 2,
						Tuple2.<TypeSerializer, KeySelector> of(StringSerializer.INSTANCE, null)), null);

		@Override
		public void collect(KVOperation<Integer, String> record) {
			elements.add(s.copy(record));
		}

		@Override
		public void close() {
		}

		@Override
		public boolean equals(Object o) {
			return o.equals(elements);
		}

		@Override
		public String toString() {
			return elements.toString();
		}
	}

	@Test
	public void keySelectorTests() throws Exception {
		assertEquals((Integer) 2,
				(Integer) (new KVUtils.KVOpKeySelector<Integer, Object>()).getKey(KVOperation.get(5, 2)));
		assertEquals((Long) 6L,
				(Long) (new KVUtils.OperationIDSelector<Integer, Object>()).getKey(KVOperation.multiGet(5, 2,
						(short) 2, 6)));
	}

	@Test
	public void toKVTest() throws Exception {
		RichMapFunction<KVOperation<Integer, String>, Tuple2<Integer, String>> toKV = new KVUtils.ToKV<>();
		toKV.open(null);

		assertEquals(Tuple2.of(2, "b"), toKV.map(KVOperation.getRes(5, 2, "b")));
		assertEquals(Tuple2.of(6, "c"), toKV.map(KVOperation.removeRes(2, 6, "c")));
	}

	@Test
	public void toSKVTest() throws Exception {
		RichMapFunction<KVOperation<Integer, String>, Tuple2<Object, String>> toSKV = new KVUtils.ToSKV<>();
		toSKV.open(null);

		assertEquals(Tuple2.of(2, "a"), toSKV.map(KVOperation.<Integer, String> selectorGetRes(5, 2, "a")));
		assertEquals(Tuple2.of("a", "b"),
				toSKV.map(KVOperation.<Integer, String> selectorGetRes(2, "a", "b")));
	}

	@Test
	public void queryIDOutputSelectorTest() {
		OutputSelector<KVOperation<String, Integer>> selector = new KVUtils.IDOutputSelector<>();

		assertEquals(Arrays.asList("2"), selector.select(KVOperation.put(2, "a", 8)));
		assertEquals(Arrays.asList("6"), selector.select(KVOperation.<String, Integer> get(6, "a")));
	}
}
