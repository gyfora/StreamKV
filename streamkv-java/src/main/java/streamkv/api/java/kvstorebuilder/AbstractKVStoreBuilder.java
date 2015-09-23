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

package streamkv.api.java.kvstorebuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import streamkv.api.java.OperationOrdering;
import streamkv.api.java.operator.AsyncKVStoreOperator;
import streamkv.api.java.operator.TimestampedKVStoreOperator;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationTypeInfo;
import streamkv.api.java.types.KVOperationTypeInfo.KVOpSerializer;
import streamkv.api.java.util.KVUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractKVStoreBuilder<K, V> {

	// Lists of input streams and query ids for the different operations, the
	// transformation is only applied when the user calls getOutputs()
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> put = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer>> update = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> get = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> remove = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer>> selectorGet = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> multiGet = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer>> selectorMultiget = new ArrayList<>();

	private int queryCount = 0;
	private OperationOrdering ordering;
	private TypeInformation inKVType;
	private ExecutionConfig config;

	private Map<Integer, DataStream> outputs = null;

	public AbstractKVStoreBuilder(OperationOrdering ordering) {
		this.ordering = ordering;
	}

	public abstract DataStream toKVStream(DataStream<KVOperation<K, V>> stream);

	public abstract DataStream toSKVStream(DataStream<KVOperation<K, V>> stream, TypeInformation recordType);

	public abstract DataStream toKVArrayStream(DataStream<KVOperation<K, V>> stream);

	public abstract DataStream toSKVArrayStream(DataStream<KVOperation<K, V>> stream,
			TypeInformation recordType);

	public abstract KVOperationTypeInfo<K, V> getKVOperationType();

	public int nextID() {
		return ++queryCount;
	}

	public TypeInformation getInKVType() {
		return inKVType;
	}

	public void put(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		if (inKVType == null) {
			inKVType = getOriginalInputType(stream);
			config = stream.getExecutionConfig();
		}
		put.add(Tuple2.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> of(stream, qid));
	}

	public void get(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		get.add(Tuple2.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> of(stream, qid));
	}

	public void remove(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		remove.add(Tuple2.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> of(stream, qid));
	}

	public void update(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, ReduceFunction<V> reducer,
			int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		if (inKVType == null) {
			inKVType = getOriginalInputType(stream);
			config = stream.getExecutionConfig();
		}
		update.add(Tuple3.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer> of(
				stream, reducer, qid));
	}

	public void multiGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		multiGet.add(Tuple2.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> of(stream, qid));
	}

	public void selectorGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, KeySelector selector,
			int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		selectorGet.add(Tuple3.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> of(
				stream, selector, qid));
	}

	public void selectorMultiGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream,
			KeySelector selector, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		selectorMultiget.add(Tuple3
				.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> of(stream,
						selector, qid));
	}

	public Map<Integer, DataStream> getOutputs() {
		if (outputs != null) {
			return outputs;
		} else {
			validateInput();

			// Convert all input types to KVOperation and group by the key field
			List<DataStream<KVOperation<K, V>>> inputStreams = getInputStreams();

			// Union all the input streams
			DataStream<KVOperation<K, V>> input = inputStreams.get(0);
			for (int i = 1; i < inputStreams.size(); i++) {
				input = input.union(inputStreams.get(i));
			}

			// Apply the operator that executes the KVStore logic then split the
			// output by their query ID
			KVOperationTypeInfo<K, V> kvOpType = getKVOperationType();
			OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> op = getKVOperator((KVOpSerializer<K, V>) kvOpType
					.createSerializer(getConfig()));
			SplitDataStream<KVOperation<K, V>> splitStream = input.transform(op.getClass().getSimpleName(),
					kvOpType, op).split(new KVUtils.IDOutputSelector<K, V>());
			Map<Integer, DataStream> outputs = new HashMap<>();
			outputs.putAll(getKVOutputs(splitStream));
			outputs.putAll(getKVArrayOutput(splitStream));
			this.outputs = outputs;
			return outputs;
		}
	}

	public ExecutionConfig getConfig() {
		return config;
	}

	private TypeInformation<?> getOriginalInputType(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream) {
		return ((OneInputTransformation) stream.getTransformation()).getInputType();
	}

	private Map<Integer, DataStream> getKVArrayOutput(SplitDataStream<KVOperation<K, V>> split) {
		Map<Integer, DataStream> keyValueArrayStreams = new HashMap<>();

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : multiGet) {
			keyValueArrayStreams.put(
					query.f1,
					toKVArrayStream(split.select(query.f1.toString()).groupBy(
							new KVUtils.OperationIDSelector<K, V>())));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorMultiget) {
			keyValueArrayStreams.put(
					query.f2,
					toSKVArrayStream(
							split.select(query.f2.toString())
									.groupBy(new KVUtils.OperationIDSelector<K, V>()),
							getOriginalInputType(query.f0)));
		}
		return keyValueArrayStreams;
	}

	private Map<Integer, DataStream> getKVOutputs(SplitDataStream<KVOperation<K, V>> split) {
		Map<Integer, DataStream> keyValueStreams = new HashMap<>();

		// For each query, we select the query ID from the SplitDataStream and
		// convert the results back from KVOperation to the proper output type
		// using a non copying map operation
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : get) {
			keyValueStreams.put(query.f1, toKVStream(split.select(query.f1.toString())));
		}

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : remove) {
			keyValueStreams.put(query.f1, toKVStream(split.select(query.f1.toString())));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorGet) {
			keyValueStreams.put(query.f2,
					toSKVStream(split.select(query.f2.toString()), getOriginalInputType(query.f0)));
		}
		return keyValueStreams;
	}

	private void checkNotFinalized() {
		if (outputs != null) {
			throw new IllegalStateException(
					"The KVStore has already been finalized by calling getOutput() on one of the queries.");
		}
	}

	private List<DataStream<KVOperation<K, V>>> getInputStreams() {
		List<DataStream<KVOperation<K, V>>> inputStreams = new ArrayList<>();

		KVOperationTypeInfo<K, V> kvOpType = getKVOperationType();

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : put) {
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer> query : update) {
			kvOpType.registerReducer(query.f2, query.f1);
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : get) {
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : remove) {
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorGet) {
			kvOpType.registerExtractor(query.f2, ((OneInputTransformation) query.f0.getTransformation())
					.getInputType().createSerializer(config), query.f1);
			final KeySelector ks = query.f1;
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KeySelector<KVOperation<K, V>, K>() {

				private static final long serialVersionUID = 8123229428587687470L;
				KeySelector selector = ks;

				@Override
				public K getKey(KVOperation<K, V> value) throws Exception {
					return (K) selector.getKey(value.getRecord());
				}

			}));
		}
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : multiGet) {
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorMultiget) {

			kvOpType.registerExtractor(query.f2, getComponentType(getOriginalInputType(query.f0))
					.createSerializer(getConfig()), query.f1);
			final KeySelector ks = query.f1;
			inputStreams.add(query.f0.returns(kvOpType).groupBy(new KeySelector<KVOperation<K, V>, K>() {

				private static final long serialVersionUID = 8123229428587687470L;
				KeySelector selector = ks;

				@Override
				public K getKey(KVOperation<K, V> value) throws Exception {
					return (K) selector.getKey(value.getRecord());
				}

			}));

		}
		return inputStreams;
	}

	private void validateInput() {
		if (put.isEmpty() && update.isEmpty()) {
			throw new RuntimeException("At least one Put or Update stream needs to be added.");
		}
	}

	protected TypeInformation getComponentType(TypeInformation arrayTypeInfo) {
		if (arrayTypeInfo instanceof PrimitiveArrayTypeInfo) {
			Class clazz = arrayTypeInfo.getClass().getComponentType();
			return TypeExtractor.getForClass(clazz);
		} else if (arrayTypeInfo instanceof BasicArrayTypeInfo) {
			return ((BasicArrayTypeInfo) arrayTypeInfo).getComponentInfo();
		} else if (arrayTypeInfo instanceof ObjectArrayTypeInfo) {
			return ((ObjectArrayTypeInfo) arrayTypeInfo).getComponentInfo();
		} else {
			throw new RuntimeException("Could not determine component type for " + arrayTypeInfo);
		}
	}

	private OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> getKVOperator(
			KVOpSerializer<K, V> serializer) {
		if (ordering == OperationOrdering.TIMESTAMP) {
			return new TimestampedKVStoreOperator<>(serializer);
		} else if (ordering == OperationOrdering.ARRIVALTIME) {
			return new AsyncKVStoreOperator<>(serializer);
		} else {
			throw new UnsupportedOperationException("Given ordering is not supported");
		}
	}
}
