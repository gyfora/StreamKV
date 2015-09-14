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

package streamkv.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.util.Collector;

import streamkv.api.KV;
import streamkv.types.KVOperation;
import streamkv.types.KVOperation.KVOperationType;

public class KVUtils {

	public static class KVOpKeySelector<K, V> implements KeySelector<KVOperation<K, V>, K> {

		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(KVOperation<K, V> op) throws Exception {
			return op.getKey();
		}

	}

	public static class ToPut<K, V> extends RichMapFunction<Tuple2<K, V>, KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToPut(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(Tuple2<K, V> next) throws Exception {
			reuse.setKey(next.f0);
			reuse.setValue(next.f1);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.PUT);
		}
	}

	public static class ToGet<K, V> extends RichMapFunction<K, KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToGet(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(K key) throws Exception {
			reuse.setKey(key);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.GET);
		}
	}

	public static class ToRemove<K, V> extends RichMapFunction<K, KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToRemove(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(K key) throws Exception {
			reuse.setKey(key);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.REMOVE);
		}
	}

	public static class ToSGet<K, V> extends RichMapFunction<Object, KVOperation<K, V>> {

		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToSGet(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(Object record) throws Exception {
			reuse.setRecord(record);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.SGET);
		}
	}

	public static class ToKV<K, V> extends RichMapFunction<KVOperation<K, V>, KV<K, V>> {

		private static final long serialVersionUID = 1L;
		private KV<K, V> reuse;

		@Override
		public KV<K, V> map(KVOperation<K, V> op) throws Exception {
			reuse.setKey(op.getKey());
			reuse.setValue(op.getValue());
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KV<>();
		}
	}

	public static class ToSKV<K, V> extends RichMapFunction<KVOperation<K, V>, KV<Object, V>> {

		private static final long serialVersionUID = 1L;
		private KV<Object, V> reuse;

		@Override
		public KV<Object, V> map(KVOperation<K, V> op) throws Exception {
			reuse.setKey(op.getRecord());
			reuse.setValue(op.getValue());
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KV<>();
		}
	}

	public static class ToMGet<K, V> extends RichFlatMapFunction<K[], KVOperation<K, V>> {

		private static final long serialVersionUID = 1L;

		private int index;
		transient private KVOperation<K, V> reuse;
		private Random rnd;

		public ToMGet(int index) {
			this.index = index;
		}

		@Override
		public void flatMap(K[] keys, Collector<KVOperation<K, V>> out) throws Exception {
			reuse.setNumKeys((short) keys.length);
			reuse.setOperationID(rnd.nextLong());
			for (K key : keys) {
				reuse.setKey(key);
				out.collect(reuse);
			}
		}

		@Override
		public void open(Configuration conf) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.MGET);
			rnd = new Random();
		}
	}

	public static class MGetMerge<K, V> extends RichFlatMapFunction<KVOperation<K, V>, KV<K, V>[]> {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("rawtypes")
		private OperatorState<Tuple2<Integer, KV[]>> merged;

		@SuppressWarnings("unchecked")
		@Override
		public void flatMap(KVOperation<K, V> next, Collector<KV<K, V>[]> out) throws Exception {
			@SuppressWarnings("rawtypes")
			Tuple2<Integer, KV[]> partial = merged.value();
			short numKeys = next.getNumKeys();

			if (numKeys == 0) {
				throw new RuntimeException("Number of keys must be at least 1");
			}

			if (partial.f1 == null) {
				partial.f0 = (int) numKeys - 1;
				partial.f1 = new KV[numKeys];
				partial.f1[0] = KV.of(next.getKey(), next.getValue());
			} else {
				partial.f0 -= 1;
				partial.f1[numKeys - partial.f0 - 1] = KV.of(next.getKey(), next.getValue());
			}

			if (partial.f0 == 0) {
				out.collect(partial.f1);
				merged.update(null);
			} else {
				merged.update(partial);
			}

		}

		@SuppressWarnings("rawtypes")
		@Override
		public void open(Configuration conf) throws IOException {
			merged = getRuntimeContext().getOperatorState("merged", Tuple2.<Integer, KV[]> of(null, null),
					true);
		}
	}

	public static class KVKeySelector<K, V> implements KeySelector<KV<K, V>, K> {

		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(KV<K, V> value) throws Exception {
			return value.getKey();
		}

	}

	public static class SelfKeyExtractor<K> implements KeySelector<K, K> {
		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(K value) throws Exception {
			return value;
		}

	}

	public static class OperationIDSelector<K, V> implements KeySelector<KVOperation<K, V>, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(KVOperation<K, V> value) throws Exception {
			return value.getOperationID();
		}
	}

	public static class IDOutputSelector<K, V> implements OutputSelector<KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		List<String> selected = Arrays.asList("0");

		@Override
		public Iterable<String> select(KVOperation<K, V> value) {
			selected.set(0, "" + value.getQueryID());
			return selected;
		}
	}
}
