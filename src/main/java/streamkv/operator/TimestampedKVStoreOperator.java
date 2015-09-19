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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import streamkv.operator.checkpointing.PendingOperationCheckpointer;
import streamkv.types.KVOperation;
import streamkv.types.KVOperationTypeInfo.KVOpSerializer;

/**
 * Time aware implementation of the KVStore operator, which executes operations
 * according to their record timestamps (which can be either ingress or custom
 * time). The operator keeps the key-value pairs partitioned among the operator
 * instances, where each partition is kept in a local {@link OperatorState} as a
 * {@link HashMap}.
 * 
 * <p>
 * Received operations are not eagerly executed, but are added to a
 * {@link TreeMap} of pending operations by their timestamp. On each watermark,
 * we execute all the operations happened before the watermark sorted by time
 * then discard them from the pending operations.
 * </p>
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public class TimestampedKVStoreOperator<K, V> extends AsyncKVStoreOperator<K, V> {

	private static final long serialVersionUID = 1L;

	private OperatorState<TreeMap<Long, List<KVOperation<K, V>>>> pendingOperations;
	private StreamRecord<KVOperation<K, V>> reuse;

	public TimestampedKVStoreOperator(KVOpSerializer<K, V> kvOpSerializer) {
		super(kvOpSerializer);
	}

	@Override
	public void processElement(StreamRecord<KVOperation<K, V>> element) throws Exception {
		addToPending(element.getValue(), element.getTimestamp());
		reuse = element;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		TreeMap<Long, List<KVOperation<K, V>>> ops = pendingOperations.value();

		Iterator<Entry<Long, List<KVOperation<K, V>>>> it = ops.headMap(mark.getTimestamp(), true).entrySet()
				.iterator();

		while (it.hasNext()) {
			Entry<Long, List<KVOperation<K, V>>> next = it.next();
			reuse.<KVOperation<K, V>> replace(null, next.getKey());
			for (KVOperation<K, V> op : next.getValue()) {
				executeOperation(op, reuse);
			}
			it.remove();
		}

		pendingOperations.update(ops);
	}

	private void addToPending(KVOperation<K, V> op, long timestamp) throws IOException {
		TreeMap<Long, List<KVOperation<K, V>>> ops = pendingOperations.value();
		List<KVOperation<K, V>> p = ops.get(timestamp);
		if (p == null) {
			p = new LinkedList<>();
			ops.put(timestamp, p);
		}
		p.add(op);
		pendingOperations.update(ops);
	}

	@Override
	public void open(Configuration c) throws IOException {
		super.open(c);
		pendingOperations = getRuntimeContext().getOperatorState("pendingOps",
				new TreeMap<Long, List<KVOperation<K, V>>>(), false,
				new PendingOperationCheckpointer<>(kvOpSerializer));
	}
}
