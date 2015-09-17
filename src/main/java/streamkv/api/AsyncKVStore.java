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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import streamkv.operator.AsyncKVStoreOperator;
import streamkv.types.KVArrayTypeInfo;
import streamkv.types.KVOperation;
import streamkv.types.KVOperationTypeInfo;
import streamkv.types.KVTypeInfo;
import streamkv.types.KVTypeInfo.KVSerializer;
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
public class AsyncKVStore<K, V> extends KVStore<K, V> {

	// Lists of input streams and query ids for the different operations, the
	// transformation is only applied when the user calls getOutputs()
	private List<Tuple2<DataStream<Tuple2<K, V>>, Integer>> put = new ArrayList<>();
	private List<Tuple3<DataStream<Tuple2<K, V>>, ReduceFunction<V>, Integer>> update = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> get = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> remove = new ArrayList<>();
	private List<Tuple3<DataStream, KeySelector, Integer>> sget = new ArrayList<>();
	private List<Tuple2<DataStream<K[]>, Integer>> multiGet = new ArrayList<>();
	private List<Tuple3<DataStream, KeySelector, Integer>> smget = new ArrayList<>();

	private boolean finalized = false;
	private int queryCount = 0;

	protected AsyncKVStore() {
	}

	@Override
	public void put(DataStream<Tuple2<K, V>> stream) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		put.add(Tuple2.of(stream, ++queryCount));
	}

	@Override
	public void update(DataStream<Tuple2<K, V>> stream, ReduceFunction<V> reducer) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		update.add(Tuple3.of(stream, reducer, ++queryCount));
	}

	@Override
	public int get(DataStream<K> stream) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		get.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	public int remove(DataStream<K> stream) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		remove.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	public <X> int getWithKeySelector(DataStream<X> stream, KeySelector<X, K> keySelector) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		sget.add(Tuple3.of((DataStream) stream, (KeySelector) keySelector, ++queryCount));
		return queryCount;
	}

	@Override
	public int multiGet(DataStream<K[]> stream) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		multiGet.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	@Override
	public <X> int multiGetWithKeySelector(DataStream<X[]> stream, KeySelector<X, K> keySelector) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		smget.add(Tuple3.of((DataStream) stream, (KeySelector) keySelector, ++queryCount));
		return queryCount;
	}

	private DataStream<Tuple2<K, V>> getKVStream() {
		if (!put.isEmpty()) {
			return put.get(0).f0;
		} else
			return update.get(0).f0;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KVStoreOutput<K, V> getOutputs() {
		finalized = true;

		if (put.isEmpty() && update.isEmpty()) {
			throw new RuntimeException("At least one Put or Update stream needs to be added.");
		}

		DataStream<Tuple2<K, V>> kvStream = getKVStream();

		// Create type information based on the inputs
		TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) kvStream.getType();
		final KVTypeInfo<K, V> kvType = new KVTypeInfo<>((TypeInformation<K>) tupleType.getTypeAt(0),
				(TypeInformation<V>) tupleType.getTypeAt(1));
		final KVOperationTypeInfo<K, V> kvOpType = new KVOperationTypeInfo<>(kvType.getKeyType(),
				kvType.getValueType());
		final ExecutionConfig config = kvStream.getExecutionEnvironment().getConfig();

		// Convert all input types to KVOperation and group by the key field
		List<DataStream<KVOperation<K, V>>> inputStreams = new ArrayList<>();

		for (Tuple2<DataStream<Tuple2<K, V>>, Integer> query : put) {
			inputStreams.add(query.f0.map(new KVUtils.ToPut<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<DataStream<Tuple2<K, V>>, ReduceFunction<V>, Integer> query : update) {
			kvOpType.registerReducer(query.f2, query.f1);
			inputStreams.add(query.f0.map(new KVUtils.ToUpdate<K, V>(query.f2)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<DataStream<K>, Integer> query : get) {
			inputStreams.add(query.f0.map(new KVUtils.ToGet<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<DataStream<K>, Integer> query : remove) {
			inputStreams.add(query.f0.map(new KVUtils.ToRemove<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			kvOpType.registerExtractor(query.f2, query.f0.getType().createSerializer(config), query.f1);
			final KeySelector ks = query.f1;
			inputStreams.add(query.f0.map(new KVUtils.ToSGet<>(query.f2)).returns(kvOpType)
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
			inputStreams.add(query.f0.flatMap(new KVUtils.ToMGet<K, V>(query.f1)).returns(kvOpType)
					.groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<DataStream, KeySelector, Integer> query : smget) {

			kvOpType.registerExtractor(query.f2,
					getComponentSerializer(query.f0.getType().createSerializer(config)), query.f1);
			final KeySelector ks = query.f1;
			inputStreams.add(query.f0.flatMap(new KVUtils.ToSMGet<K, V>(query.f2)).returns(kvOpType)
					.groupBy(new KeySelector<KVOperation<K, V>, K>() {

						private static final long serialVersionUID = 8123229428587687470L;
						KeySelector selector = ks;

						@Override
						public K getKey(KVOperation<K, V> value) throws Exception {
							return (K) selector.getKey(value.getRecord());
						}

					}));

		}

		// Union all the input streams
		DataStream<KVOperation<K, V>> input = inputStreams.get(0);
		for (int i = 1; i < inputStreams.size(); i++) {
			input = input.union(inputStreams.get(i));
		}

		// Apply the operator that executes the KVStore logic then split the
		// output by their query ID
		OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> op = getKVOperator();
		SplitDataStream<KVOperation<K, V>> split = input.transform(op.getClass().getSimpleName(), kvOpType,
				op).split(new KVUtils.IDOutputSelector<K, V>());

		// Create a map for each output stream type
		Map<Integer, DataStream> keyValueStreams = new HashMap<>();
		Map<Integer, DataStream> keyValueArrayStreams = new HashMap<>();

		// For each query, we select the query ID from the SplitDataStream and
		// convert the results back from KVOperation to the proper output type
		// using a non copying map operation

		for (Tuple2<DataStream<K>, Integer> query : get) {
			DataStream<Tuple2<K, V>> projected = KVUtils.nonCopyingMap(split.select(query.f1.toString()),
					kvType, new KVUtils.ToKV<K, V>());
			keyValueStreams.put(query.f1, projected);
		}

		for (Tuple2<DataStream<K>, Integer> query : remove) {
			DataStream<Tuple2<K, V>> projected = KVUtils.nonCopyingMap(split.select(query.f1.toString()),
					kvType, new KVUtils.ToKV<K, V>());
			keyValueStreams.put(query.f1, projected);
		}

		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			DataStream projected = KVUtils.nonCopyingMap(split.select(query.f2.toString()), new KVTypeInfo(
					query.f0.getType(), kvOpType.valueType), new KVUtils.ToSKV<K, V>());
			keyValueStreams.put(query.f2, projected);
		}

		for (Tuple2<DataStream<K[]>, Integer> query : multiGet) {
			DataStream<Tuple2[]> projected = split.select(query.f1.toString())
					.groupBy(new KVUtils.OperationIDSelector<K, V>()).flatMap(new KVUtils.MGetMerge<K, V>())
					.returns((TypeInformation<Tuple2[]>) (TypeInformation) new KVArrayTypeInfo<>(kvType));
			keyValueArrayStreams.put(query.f1, projected);
		}

		for (Tuple3<DataStream, KeySelector, Integer> query : smget) {
			DataStream<Tuple2[]> projected = split
					.select(query.f2.toString())
					.groupBy(new KVUtils.OperationIDSelector<K, V>())
					.flatMap(new KVUtils.MGetMerge<K, V>())
					.returns(
							(TypeInformation<Tuple2[]>) (TypeInformation) new KVArrayTypeInfo<>(
									new KVSerializer<>(getComponentSerializer(query.f0.getType()
											.createSerializer(config)), kvType.getValueType()
											.createSerializer(config))));
			keyValueArrayStreams.put(query.f2, projected);
		}

		return new KVStoreOutput<>(keyValueStreams, keyValueArrayStreams);

	}

	private TypeSerializer getComponentSerializer(TypeSerializer arraySerializer) {
		try {
			GenericArraySerializer gs = (GenericArraySerializer) arraySerializer;

			Field field = gs.getClass().getDeclaredField("componentSerializer");
			field.setAccessible(true);
			TypeSerializer componentSerializer = (TypeSerializer) field.get(gs);
			return componentSerializer;
		} catch (Exception e) {
			throw new RuntimeException("Could not determine component type.", e);
		}
	}

	protected void checkNotFinalized() {
		if (finalized) {
			throw new IllegalStateException("Cannot operate on the store after getting the outputs.");
		}
	}

	protected OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> getKVOperator() {
		return new AsyncKVStoreOperator<>();
	}

}
