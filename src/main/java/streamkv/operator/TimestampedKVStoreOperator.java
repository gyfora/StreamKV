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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import streamkv.types.KVOperation;

public class TimestampedKVStoreOperator<K, V> extends AsyncKVStoreOperator<K, V> {

	private static final long serialVersionUID = 1L;

	private OperatorState<TreeMap<Long, KVOperation<K, V>>> pendingOperations;
	private StreamRecord<KVOperation<K, V>> reuse;

	@Override
	public void processElement(StreamRecord<KVOperation<K, V>> element) throws Exception {
		addToPending(element.getValue(), element.getTimestamp());
		reuse = element;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		TreeMap<Long, KVOperation<K, V>> ops = pendingOperations.value();

		Iterator<Entry<Long, KVOperation<K, V>>> it = ops.headMap(mark.getTimestamp(), true).entrySet()
				.iterator();

		while (it.hasNext()) {
			executeOperation(it.next().getValue(), reuse);
			it.remove();
		}

		pendingOperations.update(ops);
	}

	private void addToPending(KVOperation<K, V> op, long timestamp) throws IOException {
		TreeMap<Long, KVOperation<K, V>> ops = pendingOperations.value();
		ops.put(timestamp, op);
		pendingOperations.update(ops);
	}

	@Override
	public void open(Configuration c) throws IOException {
		super.open(c);
		pendingOperations = getRuntimeContext().getOperatorState("pendingOps",
				new TreeMap<Long, KVOperation<K, V>>(), false);
	}
}
