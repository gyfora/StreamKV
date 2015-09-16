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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.util.Collector;

import streamkv.api.KVStore;
import streamkv.types.KVOperation;
import streamkv.types.KVOperation.KVOperationType;

import com.google.common.base.Preconditions;

/**
 * This class contains utilities for converting input and outputs to and from
 * {@link KVOperation}, and also the {@link KeySelector} implementations used by
 * the {@link KVStore}.
 * 
 */
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
			Preconditions.checkNotNull(next.f0, "Key must not be null");
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
			Preconditions.checkNotNull(key, "Key must not be null");
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
			Preconditions.checkNotNull(key, "Key must not be null");
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
			Preconditions.checkNotNull(record, "Key must not be null");

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

	public static class ToKV<K, V> extends RichMapFunction<KVOperation<K, V>, Tuple2<K, V>> {

		private static final long serialVersionUID = 1L;
		private Tuple2<K, V> reuse;

		@Override
		public Tuple2<K, V> map(KVOperation<K, V> op) throws Exception {
			reuse.setField(op.getKey(), 0);
			reuse.setField(op.getValue(), 1);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new Tuple2<>();
		}
	}

	public static class ToSKV<K, V> extends RichMapFunction<KVOperation<K, V>, Tuple2<Object, V>> {

		private static final long serialVersionUID = 1L;
		private Tuple2<Object, V> reuse;

		@Override
		public Tuple2<Object, V> map(KVOperation<K, V> op) throws Exception {
			reuse.setField(op.getRecord(), 0);
			reuse.setField(op.getValue(), 1);
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new Tuple2<>();
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
				Preconditions.checkNotNull(key, "Key must not be null");

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

	public static class ToSMGet<K, V> extends RichFlatMapFunction<Object, KVOperation<K, V>> {

		private static final long serialVersionUID = 1L;

		private int index;
		transient private KVOperation<K, V> reuse;
		private Random rnd;

		public ToSMGet(int index) {
			this.index = index;
		}

		@Override
		public void flatMap(Object in, Collector<KVOperation<K, V>> out) throws Exception {
			Object[] keys = (Object[]) in;
			reuse.setNumKeys((short) keys.length);
			reuse.setOperationID(rnd.nextLong());
			for (Object key : keys) {
				Preconditions.checkNotNull(key, "Key must not be null");
				reuse.setRecord(key);
				out.collect(reuse);
			}
		}

		@Override
		public void open(Configuration conf) {
			reuse = new KVOperation<>();
			reuse.setQueryID(index);
			reuse.setType(KVOperationType.SMGET);
			rnd = new Random();
		}
	}

	@SuppressWarnings("rawtypes")
	public static class MGetMerge<K, V> extends RichFlatMapFunction<KVOperation<K, V>, Tuple2[]> {

		private static final long serialVersionUID = 1L;

		private OperatorState<Tuple2<Integer, Tuple2[]>> merged;

		@Override
		public void flatMap(KVOperation<K, V> next, Collector<Tuple2[]> out) throws Exception {
			Tuple2<Integer, Tuple2[]> partial = merged.value();
			Object key = next.getType() == KVOperationType.MGETRES ? next.getKey() : next.getRecord();
			short numKeys = next.getNumKeys();

			if (numKeys == 0) {
				throw new RuntimeException("Number of keys must be at least 1");
			}

			if (partial.f1 == null) {
				partial.f0 = (int) numKeys - 1;
				partial.f1 = new Tuple2[numKeys];
				partial.f1[0] = Tuple2.of(key, next.getValue());

			} else {
				partial.f0 -= 1;
				partial.f1[numKeys - partial.f0 - 1] = Tuple2.of(key, next.getValue());
			}

			if (partial.f0 == 0) {
				out.collect(partial.f1);
				merged.update(null);
			} else {
				merged.update(partial);
			}

		}

		@Override
		public void open(Configuration conf) throws IOException {
			merged = getRuntimeContext().getOperatorState("merged",
					Tuple2.<Integer, Tuple2[]> of(null, null), true);
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
			selected.set(0, ((Integer) value.getQueryID()).toString());
			return selected;
		}
	}

	public static <I, O> DataStream<O> nonCopyingMap(DataStream<I> input, TypeInformation<O> outType,
			MapFunction<I, O> mapper) {
		return input.transform("NonCopyingMap", outType, new NonCopyingMap<>(mapper));
	}

	private static class NonCopyingMap<I, O> extends StreamMap<I, O> {
		private static final long serialVersionUID = 1L;

		public NonCopyingMap(MapFunction<I, O> mapper) {
			super(mapper);
			disableInputCopy();
		}
	}
}
