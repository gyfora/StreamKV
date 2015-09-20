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

package streamkv.operator.checkpointing;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

import streamkv.operator.TimestampedKVStoreOperator;
import streamkv.types.KVOperation;
import streamkv.types.KVOperationTypeInfo.KVOpSerializer;

/**
 * {@link StateCheckpointer} for the efficient serialization of the buffered
 * pending {@link KVOperation}s for {@link TimestampedKVStoreOperator}s used to
 * reduce the checkpointing overhead.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public class PendingOperationCheckpointer<K, V> implements
		StateCheckpointer<TreeMap<Long, List<KVOperation<K, V>>>, byte[]> {

	private static final long serialVersionUID = 1L;
	private KVOpSerializer<K, V> opSerializer;

	public PendingOperationCheckpointer(KVOpSerializer<K, V> opSerializer) {
		this.opSerializer = opSerializer;
	}

	@Override
	public byte[] snapshotState(TreeMap<Long, List<KVOperation<K, V>>> pending, long checkpointId,
			long checkpointTimestamp) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(pending.size() * 24);
		DataOutputView out = new OutputViewDataOutputStreamWrapper(new DataOutputStream(bos));
		try {
			out.writeInt(pending.size());
			for (Entry<Long, List<KVOperation<K, V>>> pendingEntry : pending.entrySet()) {
				out.writeLong(pendingEntry.getKey());
				List<KVOperation<K, V>> pendingList = pendingEntry.getValue();
				out.writeInt(pendingList.size());
				for (KVOperation<K, V> op : pendingList) {
					opSerializer.serialize(op, out);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to write snapshot", e);
		}
		return bos.toByteArray();
	}

	@Override
	public TreeMap<Long, List<KVOperation<K, V>>> restoreState(byte[] stateSnapshot) {
		ByteArrayInputView in = new ByteArrayInputView(stateSnapshot);

		TreeMap<Long, List<KVOperation<K, V>>> returnMap = new TreeMap<>();
		try {
			int mapSize = in.readInt();
			for (int i = 0; i < mapSize; i++) {
				long ts = in.readLong();
				int listSize = in.readInt();
				LinkedList<KVOperation<K, V>> pending = new LinkedList<>();
				for (int j = 0; j < listSize; j++) {
					pending.add(opSerializer.deserialize(in));
				}
				returnMap.put(ts, pending);
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to read snapshot", e);
		}

		return returnMap;
	}

}
