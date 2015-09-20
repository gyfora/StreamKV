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

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

/**
 * {@link StateCheckpointer} for the efficient serialization of the intermediate
 * merged values for multiget operations.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MergeStateCheckpointer<K, V> implements StateCheckpointer<Tuple2<Integer, Tuple2[]>, byte[]> {

	private static final long serialVersionUID = 1L;
	TypeSerializer keySerializer;
	TypeSerializer valueSerializer;

	public MergeStateCheckpointer(TypeSerializer keySerializer, TypeSerializer valueSerializer) {
		super();
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public byte[] snapshotState(Tuple2<Integer, Tuple2[]> state, long checkpointId, long checkpointTimestamp) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(4 + state.f1.length * (12));
		DataOutputView out = new OutputViewDataOutputStreamWrapper(new DataOutputStream(bos));
		try {
			out.writeInt(state.f0);
			out.writeInt(state.f1.length);
			for (Tuple2 t : state.f1) {
				if (t != null) {
					out.writeBoolean(true);
					keySerializer.serialize(t.f0, out);
					if (t.f1 == null) {
						out.writeBoolean(false);
					} else {
						out.writeBoolean(true);
						valueSerializer.serialize(t.f1, out);
					}
				} else {
					out.writeBoolean(false);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to write snapshot, e");
		}
		return bos.toByteArray();
	}

	@Override
	public Tuple2<Integer, Tuple2[]> restoreState(byte[] stateSnapshot) {
		ByteArrayInputView in = new ByteArrayInputView(stateSnapshot);

		Tuple2<Integer, Tuple2[]> outTuple = new Tuple2<>();
		try {
			outTuple.f0 = in.readInt();

			int len = in.readInt();
			Tuple2[] arr = new Tuple2[len];
			for (int i = 0; i < len; i++) {
				if (in.readBoolean()) {
					arr[i] = Tuple2.of(keySerializer.deserialize(in),
							in.readBoolean() ? valueSerializer.deserialize(in) : null);
				} else {
					arr[i] = null;
				}
			}
			outTuple.f1 = arr;
		} catch (IOException e) {
			throw new RuntimeException("Failed to read snapshot", e);
		}
		return outTuple;
	}
}
