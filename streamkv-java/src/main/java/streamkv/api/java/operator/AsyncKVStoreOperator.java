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

package streamkv.api.java.operator;

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import streamkv.api.java.operator.checkpointing.KVMapCheckpointer;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationTypeInfo.KVOpSerializer;

/**
 * Asynchronous implementation of the KVStore operator, which executes
 * operations in arrival order. The operator keeps the key-value pairs
 * partitioned among the operator instances, where each partition is kept in a
 * local {@link OperatorState} as a {@link HashMap}.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
@SuppressWarnings("rawtypes")
public class AsyncKVStoreOperator<K, V> extends AbstractUdfStreamOperator<KVOperation<K, V>, MapFunction> implements
		OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;

	private OperatorState<HashMap<K, V>> kvStore;
	protected KVOpSerializer<K, V> kvOpSerializer;

	public AsyncKVStoreOperator(KVOpSerializer<K, V> kvOpSerializer) {
		super(null);
		this.kvOpSerializer = kvOpSerializer;
	}

	@Override
	public void processElement(StreamRecord<KVOperation<K, V>> element) throws Exception {
		executeOperation(element.getValue(), element);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
	}

	protected void executeOperation(KVOperation<K, V> op, StreamRecord<KVOperation<K, V>> reuse)
			throws Exception {
		HashMap<K, V> store = kvStore.value();
		K key = op.getKey();

		switch (op.getType()) {
		case PUT:
			store.put(key, op.getValue());
			break;
		case UPDATE:
			ReduceFunction<V> reduceFunction = op.getReducer();
			if (!store.containsKey(key)) {
				store.put(key, op.getValue());
			} else {
				// FIXME shall we copy here?
				store.put(key, reduceFunction.reduce(store.get(key), op.getValue()));
			}
			break;
		case GET:
			output.collect(reuse.replace(KVOperation.getRes(op.getQueryID(), key, store.get(key))));
			break;
		case MGET:
			output.collect(reuse.replace(KVOperation.multiGetRes(op.getQueryID(), key, store.get(key),
					op.getNumKeys(), op.getOperationID())));
			break;
		case REMOVE:
			output.collect(reuse.replace(KVOperation.removeRes(op.getQueryID(), key, store.remove(key))));
			break;
		case SGET:
			Object record = op.getRecord();
			KeySelector<Object, K> selector = op.getKeySelector();
			output.collect(reuse.replace(KVOperation.<K, V> selectorGetRes(op.getQueryID(), record,
					store.get(selector.getKey(record)))));
			break;
		case SMGET:
			Object rec = op.getRecord();
			KeySelector<Object, K> s = op.getKeySelector();
			output.collect(reuse.replace(KVOperation.<K, V> selectorMultiGetRes(op.getQueryID(), rec,
					store.get(s.getKey(rec)), op.getNumKeys(), op.getOperationID())));
			break;
		default:
			throw new UnsupportedOperationException("Not implemented yet");
		}

		kvStore.update(store);
	}

	@Override
	public void open(Configuration c) throws IOException {
		kvStore = getRuntimeContext().getOperatorState("kv-store", new HashMap<K, V>(), false,
				new KVMapCheckpointer<>(kvOpSerializer.keySerializer, kvOpSerializer.valueSerializer));
	}
}
