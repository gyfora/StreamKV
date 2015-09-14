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

package streamkv.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import streamkv.types.KVOperation.KVOperationType;

public class KVOperationTypeInfo<K, V> extends TypeInformation<KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;

	public TypeInformation<K> keyType;
	public TypeInformation<V> valueType;
	@SuppressWarnings("rawtypes")
	Map<Integer, Tuple2<TypeInformation, KeySelector>> selectors = new HashMap<>();

	public KVOperationTypeInfo(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	@SuppressWarnings("rawtypes")
	public void registerExtractor(int qID, TypeInformation inType, KeySelector key) {
		selectors.put(qID, Tuple2.of(inType, key));
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public int getTotalFields() {
		return 0;
	}

	@Override
	public Class<KVOperation<K, V>> getTypeClass() {
		return null;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<KVOperation<K, V>> createSerializer(ExecutionConfig config) {
		return new KVOpSerializer<>(keyType.createSerializer(config), valueType == null ? null
				: valueType.createSerializer(config), selectors, config);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof KVOperationTypeInfo) {
			try {
				@SuppressWarnings("unchecked")
				KVOperationTypeInfo<K, V> otherType = (KVOperationTypeInfo<K, V>) other;
				return keyType.equals(otherType.keyType) && valueType.equals(otherType.valueType)
						&& selectors.equals(otherType.selectors);
			} catch (Exception e) {
				return false;
			}
		} else {
			return false;
		}
	}

	@SuppressWarnings("rawtypes")
	public static class KVOpSerializer<K, V> extends TypeSerializer<KVOperation<K, V>> {

		private static final long serialVersionUID = 1L;
		private TypeSerializer<K> keySerializer;
		private TypeSerializer<V> valueSerializer;
		private Map<Integer, Tuple2<TypeSerializer, KeySelector>> selectors;

		public KVOpSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
				Map<Integer, Tuple2<TypeInformation, KeySelector>> selectorInfo, ExecutionConfig config) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
			if (selectorInfo != null) {
				selectors = new HashMap<>();
				for (Entry<Integer, Tuple2<TypeInformation, KeySelector>> entry : selectorInfo.entrySet()) {
					selectors.put(entry.getKey(),
							Tuple2.of(entry.getValue().f0.createSerializer(config), entry.getValue().f1));
				}
			}
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<KVOperation<K, V>> duplicate() {
			return this;
		}

		@Override
		public KVOperation<K, V> createInstance() {
			return new KVOperation<>();
		}

		@Override
		public KVOperation<K, V> copy(KVOperation<K, V> from) {
			return copy(from, createInstance());
		}

		@SuppressWarnings("unchecked")
		@Override
		public KVOperation<K, V> copy(KVOperation<K, V> from, KVOperation<K, V> to) {
			KVOperationType type = from.getType();

			to.setType(type);
			to.setQueryID(from.getQueryID());

			switch (type) {
			case PUT:
			case GETRES:
			case REMOVERES:
				to.setKey(keySerializer.copy(from.getKey(), to.getKey()));
				to.setValue(valueSerializer.copy(from.getValue(), to.getValue()));
				break;
			case GET:
			case REMOVE:
				to.setKey(keySerializer.copy(from.getKey(), to.getKey()));
				break;
			case MGET:
				to.setKey(keySerializer.copy(from.getKey(), to.getKey()));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
				break;
			case MGETRES:
				to.setKey(keySerializer.copy(from.getKey(), to.getKey()));
				to.setValue(valueSerializer.copy(from.getValue(), to.getValue()));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
				break;
			case SGET:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(selectors.get(from.getQueryID()).f0.copy(from.getRecord()));
				break;
			case SGETRES:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(selectors.get(from.getQueryID()).f0.copy(from.getRecord()));
				to.setValue(valueSerializer.copy(from.getValue(), to.getValue()));
				break;
			default:
				throw new UnsupportedOperationException();
			}

			return to;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void serialize(KVOperation<K, V> op, DataOutputView target) throws IOException {

			target.writeInt(op.getQueryID());

			switch (op.getType()) {
			case PUT:
				target.writeShort(0);
				keySerializer.serialize(op.getKey(), target);
				valueSerializer.serialize(op.getValue(), target);
				break;
			case GET:
				target.writeShort(1);
				keySerializer.serialize(op.getKey(), target);
				break;
			case REMOVE:
				target.writeShort(2);
				keySerializer.serialize(op.getKey(), target);
				break;
			case MGET:
				target.writeShort(3);
				keySerializer.serialize(op.getKey(), target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case SGET:
				target.writeShort(4);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				break;
			case GETRES:
				target.writeShort(5);
				keySerializer.serialize(op.getKey(), target);
				serializeVal(op, target);
				break;
			case REMOVERES:
				target.writeShort(6);
				keySerializer.serialize(op.getKey(), target);
				serializeVal(op, target);
				break;
			case MGETRES:
				target.writeShort(7);
				keySerializer.serialize(op.getKey(), target);
				serializeVal(op, target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case SGETRES:
				target.writeShort(8);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				serializeVal(op, target);
				break;
			default:
				throw new RuntimeException("Invalid operation: " + op.getType().name());
			}
		}

		private void serializeVal(KVOperation<K, V> op, DataOutputView target) throws IOException {
			boolean hasVal = op.getValue() != null;
			target.writeBoolean(hasVal);
			if (hasVal) {
				valueSerializer.serialize(op.getValue(), target);
			}
		}

		private V deserializeVal(V reuse, DataInputView source) throws IOException {
			if (source.readBoolean()) {
				return valueSerializer.deserialize(reuse, source);
			} else {
				return null;
			}
		}

		@Override
		public KVOperation<K, V> deserialize(DataInputView source) throws IOException {
			return deserialize(createInstance(), source);
		}

		@SuppressWarnings("unchecked")
		@Override
		public KVOperation<K, V> deserialize(KVOperation<K, V> op, DataInputView source) throws IOException {
			op.setQueryID(source.readInt());

			KVOperationType type = KVOperation.types[source.readShort()];

			op.setType(type);

			switch (type) {
			case PUT:
				op.setKey(keySerializer.deserialize(source));
				op.setValue(valueSerializer.deserialize(source));
				break;
			case GET:
				op.setKey(keySerializer.deserialize(source));
				break;
			case REMOVE:
				op.setKey(keySerializer.deserialize(source));
				break;
			case MGET:
				op.setKey(keySerializer.deserialize(source));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case SGET:
				Tuple2<TypeSerializer, KeySelector> selector = selectors.get(op.getQueryID());
				op.setKeySelector(selector.f1);
				op.setRecord(selector.f0.deserialize(source));
				break;
			case GETRES:
				op.setKey(keySerializer.deserialize(source));
				op.setValue(deserializeVal(op.getValue(), source));
				break;
			case REMOVERES:
				op.setKey(keySerializer.deserialize(source));
				op.setValue(deserializeVal(op.getValue(), source));
				break;
			case MGETRES:
				op.setKey(keySerializer.deserialize(source));
				op.setValue(deserializeVal(op.getValue(), source));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case SGETRES:
				op.setRecord(selectors.get(op.getQueryID()).f0.deserialize(source));
				op.setValue(deserializeVal(op.getValue(), source));
				break;
			default:
				break;
			}
			return op;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("Not implemented yet");
		}

	}

}
