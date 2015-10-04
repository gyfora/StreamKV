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
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import streamkv.api.java.OperationOrdering;
import streamkv.api.java.operator.TimestampedKVStoreOperator;
import streamkv.api.java.operator.AsyncKVStoreOperator;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationSerializer;
import streamkv.api.java.types.KVOperationTypeInfo;
import streamkv.api.java.util.KVUtils;

import com.google.common.base.Preconditions;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractKVStoreBuilder<K, V> {

	// Lists of input streams and query ids for the different operations, the
	// transformation is only applied when the user calls getOutputs()
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> put = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer>> update = new ArrayList<>();
	protected List<Tuple4<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, KeySelector, Integer>> selectorUpdate = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> get = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> remove = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer>> selectorGet = new ArrayList<>();
	protected List<Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer>> multiGet = new ArrayList<>();
	protected List<Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer>> selectorMultiget = new ArrayList<>();
	protected List<Tuple2<DataStream<KVOperation<K, V>>, Integer>> opStream = new ArrayList<>();

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

	public abstract DataStream toSKVArrayStream(DataStream<KVOperation<K, V>> stream, TypeInformation recordType);

	public abstract KVOperationTypeInfo<K, V> getKVOperationType();

	public int nextID() {
		return ++queryCount;
	}

	public TypeInformation getInKVType() {
		return inKVType;
	}

	public void setInKVType(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		inKVType = new TupleTypeInfo<>(Tuple2.class, keyType, valueType);
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

	public void update(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, ReduceFunction<V> reducer, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		if (inKVType == null) {
			inKVType = getOriginalInputType(stream);
			config = stream.getExecutionConfig();
		}
		update.add(Tuple3.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer> of(stream,
				reducer, qid));
	}

	public void multiGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		multiGet.add(Tuple2.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> of(stream, qid));
	}

	public void selectorGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, KeySelector selector, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		selectorGet.add(Tuple3.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> of(stream,
				selector, qid));
	}

	public void selectorUpdate(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, KeySelector selector,
			ReduceFunction<V> reducer, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		selectorUpdate.add(Tuple4
				.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, KeySelector, Integer> of(stream,
						reducer, selector, qid));
	}

	public void selectorMultiGet(SingleOutputStreamOperator<KVOperation<K, V>, ?> stream, KeySelector selector, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		selectorMultiget.add(Tuple3.<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> of(stream,
				selector, qid));
	}

	public void applyOperation(DataStream<KVOperation<K, V>> stream, int qid) {
		Preconditions.checkNotNull(stream, "Input stream must not be null.");
		checkNotFinalized();
		opStream.add(Tuple2.<DataStream<KVOperation<K, V>>, Integer> of(stream, qid));
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
			OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> op = getKVOperator((KVOperationSerializer<K, V>) kvOpType
					.createSerializer(getConfig()));
			SplitStream<KVOperation<K, V>> splitStream = null;

			if (opStream.isEmpty()) {
				splitStream = input.transform(op.getClass().getSimpleName(), kvOpType, op).split(
						new KVUtils.StoreOutputSplitter<K, V>());
			} else {
				IterativeStream<KVOperation<K, V>> it = input.iterate(3000);

				splitStream = it.transform(op.getClass().getSimpleName(), kvOpType, op).split(
						new KVUtils.StoreOutputSplitter<K, V>());

				it.closeWith(splitStream.select("back").partitionByHash(new KVUtils.DependentKeySelector<K, V>()));
			}

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

	private Map<Integer, DataStream> getKVArrayOutput(SplitStream<KVOperation<K, V>> split) {
		Map<Integer, DataStream> keyValueArrayStreams = new HashMap<>();

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : multiGet) {
			keyValueArrayStreams.put(query.f1,
					toKVArrayStream(split.select(query.f1.toString()).keyBy(new KVUtils.OperationIDSelector<K, V>())));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorMultiget) {
			keyValueArrayStreams.put(
					query.f2,
					toSKVArrayStream(split.select(query.f2.toString()).keyBy(new KVUtils.OperationIDSelector<K, V>()),
							getOriginalInputType(query.f0)));
		}
		return keyValueArrayStreams;
	}

	private Map<Integer, DataStream> getKVOutputs(SplitStream<KVOperation<K, V>> split) {
		Map<Integer, DataStream> keyValueStreams = new HashMap<>();

		// For each query, we select the query ID from the SplitStream and
		// convert the results back from KVOperation to the proper output type
		// using a non copying map operation
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : get) {
			keyValueStreams.put(query.f1, toKVStream(split.select(query.f1.toString())));
		}

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : remove) {
			keyValueStreams.put(query.f1, toKVStream(split.select(query.f1.toString())));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer> query : update) {
			keyValueStreams.put(query.f2, toKVStream(split.select(query.f2.toString())));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorGet) {
			keyValueStreams.put(query.f2,
					toSKVStream(split.select(query.f2.toString()), getOriginalInputType(query.f0)));
		}

		for (Tuple4<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, KeySelector, Integer> query : selectorUpdate) {
			keyValueStreams.put(
					query.f3,
					toSKVStream(split.select(query.f3.toString()),
							((CompositeType) getOriginalInputType(query.f0)).getTypeAt(0)));
		}

		for (Tuple2<DataStream<KVOperation<K, V>>, Integer> query : opStream) {
			keyValueStreams.put(query.f1, split.select(query.f1.toString()));
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
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.KVOpKeySelector<K, V>()));
		}

		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, Integer> query : update) {
			kvOpType.registerReducer(query.f2, query.f1);
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.KVOpKeySelector<K, V>()));
		}

		for (Tuple4<SingleOutputStreamOperator<KVOperation<K, V>, ?>, ReduceFunction<V>, KeySelector, Integer> query : selectorUpdate) {
			kvOpType.registerReducer(query.f3, query.f1);
			kvOpType.registerExtractor(
					query.f3,
					((CompositeType) ((OneInputTransformation) query.f0.getTransformation()).getInputType()).getTypeAt(
							0).createSerializer(config), query.f2);
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.RecordSelector<K, V>(query.f2)));
		}

		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : get) {
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : remove) {
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorGet) {
			kvOpType.registerExtractor(query.f2, ((OneInputTransformation) query.f0.getTransformation()).getInputType()
					.createSerializer(config), query.f1);
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.RecordSelector<K, V>(query.f1)));
		}
		for (Tuple2<SingleOutputStreamOperator<KVOperation<K, V>, ?>, Integer> query : multiGet) {
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.KVOpKeySelector<K, V>()));
		}
		for (Tuple3<SingleOutputStreamOperator<KVOperation<K, V>, ?>, KeySelector, Integer> query : selectorMultiget) {

			kvOpType.registerExtractor(query.f2,
					getComponentType(getOriginalInputType(query.f0)).createSerializer(getConfig()), query.f1);
			inputStreams.add(query.f0.returns(kvOpType).keyBy(new KVUtils.RecordSelector<K, V>(query.f1)));

		}

		for (Tuple2<DataStream<KVOperation<K, V>>, Integer> query : opStream) {
			inputStreams.add(((SingleOutputStreamOperator<KVOperation<K, V>, ?>) query.f0).returns(kvOpType).keyBy(
					new KVUtils.ApplyOpKeySelector<K, V>()));
		}
		return inputStreams;
	}

	private void validateInput() {
		if (inKVType == null) {
			throw new RuntimeException("Input type could not be determined.");
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
			KVOperationSerializer<K, V> serializer) {
		if (ordering == OperationOrdering.TIMESTAMP) {
			return new TimestampedKVStoreOperator<>(serializer);
		} else if (ordering == OperationOrdering.ARRIVALTIME) {
			return new AsyncKVStoreOperator<>(serializer);
		} else {
			throw new UnsupportedOperationException("Given ordering is not supported");
		}
	}
}
