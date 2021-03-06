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

package streamkv.api.java.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.util.Collector;

import streamkv.api.java.KVStore;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperation.KVOperationType;
import streamkv.api.java.types.NullHandlerTypeInfo;

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
			return op.key;
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
			Preconditions.checkNotNull(next.f1, "Value must not be null");
			reuse.key = next.f0;
			reuse.value = next.f1;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = ((short) index);
			reuse.type = KVOperationType.PUT;
		}
	}

	public static class ToUpdate<K, V> extends RichMapFunction<Tuple2<K, V>, KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToUpdate(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(Tuple2<K, V> next) throws Exception {
			Preconditions.checkNotNull(next.f0, "Key must not be null");
			Preconditions.checkNotNull(next.f1, "Value must not be null");
			reuse.key = next.f0;
			reuse.value = next.f1;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.UPDATE;
		}
	}

	public static class ToSUpdate<K, V> extends RichMapFunction<Tuple2<K, V>, KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		private int index;
		private KVOperation<K, V> reuse;

		public ToSUpdate(int index) {
			this.index = index;
		}

		@Override
		public KVOperation<K, V> map(Tuple2<K, V> next) throws Exception {
			Preconditions.checkNotNull(next.f0, "Key must not be null");
			Preconditions.checkNotNull(next.f1, "Value must not be null");
			reuse.record = next.f0;
			reuse.value = next.f1;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.SUPDATE;
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
			reuse.key = key;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.GET;
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
			reuse.key = key;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.REMOVE;
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

			reuse.record = record;
			return reuse;
		}

		@Override
		public void open(Configuration c) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.SGET;
		}
	}

	public static class ToKV<K, V> extends RichMapFunction<KVOperation<K, V>, Tuple2<K, V>> {

		private static final long serialVersionUID = 1L;
		private Tuple2<K, V> reuse;

		@Override
		public Tuple2<K, V> map(KVOperation<K, V> op) throws Exception {
			reuse.f0 = op.key;
			reuse.f1 = op.value;
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
			reuse.f0 = op.record;
			reuse.f1 = op.value;
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
			if (keys.length == 0) {
				throw new RuntimeException("Number of keys must be at least 1.");
			}
			reuse.numKeys = (short) keys.length;
			reuse.operationID = rnd.nextLong();
			short c = 0;
			for (K key : keys) {
				Preconditions.checkNotNull(key, "Key must not be null");
				reuse.key = key;
				reuse.index = c++;
				out.collect(reuse);
			}
		}

		@Override
		public void open(Configuration conf) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.hasOpId = true;
			reuse.type = KVOperationType.MGET;
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
			if (keys.length == 0) {
				throw new RuntimeException("Number of keys must be at least 1.");
			}
			reuse.numKeys = (short) keys.length;
			reuse.operationID = rnd.nextLong();
			short c = 0;
			for (Object key : keys) {
				Preconditions.checkNotNull(key, "Key must not be null");
				reuse.record = key;
				reuse.index = c++;
				out.collect(reuse);
			}
		}

		@Override
		public void open(Configuration conf) {
			reuse = new KVOperation<>();
			reuse.queryID = (short) index;
			reuse.type = KVOperationType.SMGET;
			reuse.hasOpId = true;
			rnd = new Random();
		}
	}

	public static class OperationIDSelector<K, V> implements KeySelector<KVOperation<K, V>, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(KVOperation<K, V> value) throws Exception {
			return value.operationID;
		}
	}

	public static class StoreOutputSplitter<K, V> implements OutputSelector<KVOperation<K, V>> {
		private static final long serialVersionUID = 1L;
		List<String> selected = Arrays.asList("0");

		@Override
		public Iterable<String> select(KVOperation<K, V> value) {
			if (value.dependentKey != null) {
				selected.set(0, "back");
			} else {
				selected.set(0, ((Short) value.queryID).toString());
			}
			return selected;
		}
	}

	public static <I, O> SingleOutputStreamOperator<O, ?> nonCopyingMap(DataStream<I> input,
			TypeInformation<O> outType, MapFunction<I, O> mapper) {
		return input.transform("NonCopyingMap", outType, new NonCopyingMap<>(mapper));
	}

	private static class NonCopyingMap<I, O> extends StreamMap<I, O> {
		private static final long serialVersionUID = 1L;

		public NonCopyingMap(MapFunction<I, O> mapper) {
			super(mapper);
			disableInputCopy();
		}
	}

	public static <K, V> TypeInformation<Tuple2<K, V>> getKVType(TypeInformation<K> keyType,
			TypeInformation<V> valueType) {
		return new TupleTypeInfo<>(keyType, new NullHandlerTypeInfo<>(valueType));
	}

	public static class ApplyOpKeySelector<K, V> implements KeySelector<KVOperation<K, V>, K> {

		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(KVOperation<K, V> op) throws Exception {
			if (op.type.isResult && op.dependentKey != null) {
				return op.dependentKey;
			} else {
				return getK(op);
			}
		}

	}

	public static class NoOpMap<K, V> implements MapFunction<KVOperation<K, V>, KVOperation<K, V>> {
		private static final long serialVersionUID = 1;

		@Override
		public KVOperation<K, V> map(KVOperation<K, V> value) throws Exception {
			return value;
		}
	}

	public static class DependentKeySelector<K, V> implements KeySelector<KVOperation<K, V>, K> {

		private static final long serialVersionUID = 1L;

		@Override
		public K getKey(KVOperation<K, V> value) throws Exception {
			return value.dependentKey;
		}

	}

	public static <K, V> K getK(KVOperation<K, V> op) throws Exception {
		switch (op.type) {
		case PUT:
		case UPDATE:
		case GET:
		case MGET:
		case REMOVE:
		case LOCK:
		case KVRES:
		case MGETRES:
			return op.key;
		case SGET:
		case SGETRES:
		case SMGET:
		case SKVRES:
		case SUPDATE:
			return op.keySelector.getKey(op.record);
		default:
			throw new UnsupportedOperationException("Not implemented yet");
		}

	}

	@SuppressWarnings("rawtypes")
	public static class RecordSelector<K, V> implements KeySelector<KVOperation<K, V>, K> {

		private static final long serialVersionUID = 8123229428587687470L;
		private KeySelector selector;

		public RecordSelector(KeySelector ks) {
			this.selector = ks;
		}

		@SuppressWarnings("unchecked")
		@Override
		public K getKey(KVOperation<K, V> value) throws Exception {
			return (K) selector.getKey(value.record);
		}

	}
}
