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

package streamkv.operator;

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import streamkv.types.KVOperation;

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
public class AsyncKVStoreOperator<K, V> extends AbstractStreamOperator<KVOperation<K, V>> implements
		OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;

	private OperatorState<HashMap<K, V>> kvStore;

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
			output.collect(reuse.replace(KVOperation.<K,V>selectorGetRes(op.getQueryID(),
					record, store.get(selector.getKey(record)))));
			break;
		default:
			throw new UnsupportedOperationException("Not implemented yet");
		}

		kvStore.update(store);
	}

	@Override
	public void open(Configuration c) throws IOException {
		kvStore = getRuntimeContext().getOperatorState("kv-store", new HashMap<K, V>(), false);
	}
}
