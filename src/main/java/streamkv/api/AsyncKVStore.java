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

package streamkv.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import streamkv.operator.AsyncKVStoreOperator;
import streamkv.types.KVArrayTypeInfo;
import streamkv.types.KVOperation;
import streamkv.types.KVOperationTypeInfo;
import streamkv.types.KVTypeInfo;
import streamkv.util.KVUtils;

/**
 * Fully asynchronous {@link KVStore} implementation where all operations are
 * executed in arrival order (governed by the standard Flink partial ordering
 * guarantees). While this implementation provides maximal performance it does
 * not provide any deterministic processing guarantee.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
@SuppressWarnings("rawtypes")
public class AsyncKVStore<K, V> implements KVStore<K, V> {
	private List<Tuple2<DataStream<Tuple2<K, V>>, Integer>> put = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> get = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> remove = new ArrayList<>();
	private List<Tuple3<DataStream, KeySelector, Integer>> sget = new ArrayList<>();
	private List<Tuple2<DataStream<K[]>, Integer>> multiGet = new ArrayList<>();

	private List<DataStream<KVOperation<K, V>>> opStream = new ArrayList<>();
	private boolean finalized = false;

	private int queryCount = 0;

	@Override
	public void put(DataStream<Tuple2<K, V>> stream) {
		checkNotFinalized();
		put.add(Tuple2.of(stream, ++queryCount));
	}

	@Override
	public int get(DataStream<K> stream) {
		checkNotFinalized();
		get.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	public int remove(DataStream<K> stream) {
		checkNotFinalized();
		remove.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	public <X> int getWithKeySelector(DataStream<X> stream, KeySelector<X, K> keySelector) {
		checkNotFinalized();
		sget.add(Tuple3.of((DataStream) stream, (KeySelector) keySelector, ++queryCount));
		return queryCount;
	}

	@Override
	public int multiGet(DataStream<K[]> stream) {
		checkNotFinalized();
		multiGet.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KVStoreOutput<K, V> getOutputs() {
		finalized = true;
		if (put.isEmpty()) {
			throw new RuntimeException("At least one Put stream needs to be added.");
		}
		final TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) put.get(0).f0.getType();
		final KVTypeInfo<K, V> kvType = new KVTypeInfo<>((TypeInformation<K>) tupleType.getTypeAt(0),
				(TypeInformation<V>) tupleType.getTypeAt(1));
		final KVOperationTypeInfo<K, V> kvOpType = new KVOperationTypeInfo<>(kvType.getKeyType(),
				kvType.getValueType());

		for (Tuple2<DataStream<Tuple2<K, V>>, Integer> query : put) {
			opStream.add(query.f0.map(new KVUtils.ToPut<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<DataStream<K>, Integer> query : get) {
			opStream.add(query.f0.map(new KVUtils.ToGet<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<DataStream<K>, Integer> query : remove) {
			opStream.add(query.f0.map(new KVUtils.ToRemove<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			kvOpType.registerExtractor(query.f2, query.f0.getType(), query.f1);
			final KeySelector ks = query.f1;
			opStream.add(query.f0.map(new KVUtils.ToSGet<>(query.f2)).returns(kvOpType)
					.groupBy(new KeySelector<KVOperation<K, V>, K>() {

						private static final long serialVersionUID = 8123229428587687470L;
						KeySelector selector = ks;

						@Override
						public K getKey(KVOperation<K, V> value) throws Exception {
							return (K) selector.getKey(value.getRecord());
						}

					}));
		}
		for (Tuple2<DataStream<K[]>, Integer> query : multiGet) {
			opStream.add(query.f0.flatMap(new KVUtils.ToMGet<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}

		DataStream<KVOperation<K, V>> input = opStream.get(0);
		for (int i = 1; i < opStream.size(); i++) {
			input = input.union(opStream.get(i));
		}

		SplitDataStream<KVOperation<K, V>> split = input.transform("KVStore", kvOpType, getKVOperator())
				.split(new KVUtils.IDOutputSelector<K, V>());

		Map<Integer, DataStream<KV<K, V>>> kvStreams = new HashMap<>();
		Map<Integer, DataStream> skvStreams = new HashMap<>();
		Map<Integer, DataStream<KV<K, V>[]>> mkvStreams = new HashMap<>();

		for (Tuple2<DataStream<K>, Integer> query : get) {
			DataStream<KV<K, V>> projected = split.select(query.f1.toString()).map(new KVUtils.ToKV<K, V>())
					.returns(new KVTypeInfo<>(kvOpType.keyType, kvOpType.valueType));
			kvStreams.put(query.f1, projected);
		}

		for (Tuple2<DataStream<K>, Integer> query : remove) {
			DataStream<KV<K, V>> projected = split.select(query.f1.toString()).map(new KVUtils.ToKV<K, V>())
					.returns(kvType);
			kvStreams.put(query.f1, projected);
		}

		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			DataStream projected = split.select(query.f2.toString()).map(new KVUtils.ToSKV<K, V>())
					.returns(new KVTypeInfo(query.f0.getType(), kvOpType.valueType));
			skvStreams.put(query.f2, projected);
		}

		for (Tuple2<DataStream<K[]>, Integer> query : multiGet) {
			DataStream<KV<K, V>[]> projected = split.select(query.f1.toString())
					.groupBy(new KVUtils.OperationIDSelector<K, V>()).flatMap(new KVUtils.MGetMerge<K, V>())
					.returns(new KVArrayTypeInfo<>(kvType));
			mkvStreams.put(query.f1, projected);
		}

		return new KVStoreOutput<>(kvStreams, skvStreams, mkvStreams);

	}

	protected void checkNotFinalized() {
		if (finalized) {
			throw new IllegalStateException("Cannot operate on the Store after getting the outputs.");
		}
	}

	protected OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> getKVOperator() {
		return new AsyncKVStoreOperator<>();
	}

}
