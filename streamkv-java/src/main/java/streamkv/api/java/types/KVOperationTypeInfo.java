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

package streamkv.api.java.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import streamkv.api.java.types.KVOperation.KVOperationType;

public class KVOperationTypeInfo<K, V> extends TypeInformation<KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;

	public TypeInformation<K> keyType;
	public TypeInformation<V> valueType;
	@SuppressWarnings("rawtypes")
	Map<Short, Tuple2<TypeSerializer, KeySelector>> selectors = new HashMap<>();

	Map<Short, ReduceFunction<V>> reducers = new HashMap<>();

	public KVOperationTypeInfo(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	@SuppressWarnings("rawtypes")
	public void registerExtractor(int qID, TypeSerializer inType, KeySelector key) {
		selectors.put((short) qID, Tuple2.of(inType, key));
	}

	public void registerReducer(int qID, ReduceFunction<V> reduceFunction) {
		reducers.put((short) qID, reduceFunction);
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
				: valueType.createSerializer(config), reducers, selectors, config);
	}
	
	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof KVOperationTypeInfo;
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

	@Override
	public int hashCode() {
		int result = keyType != null ? keyType.hashCode() : 0;
		result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
		result = 31 * result + (selectors != null ? selectors.hashCode() : 0);
		result = 31 * result + (reducers != null ? reducers.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "KVOperationTypeInfo{" +
				"keyType=" + keyType +
				", valueType=" + valueType +
				", selectors=" + selectors +
				", reducers=" + reducers +
				'}';
	}

	@SuppressWarnings("rawtypes")
	public static class KVOpSerializer<K, V> extends TypeSerializer<KVOperation<K, V>> {

		private static final long serialVersionUID = 1L;
		public TypeSerializer<K> keySerializer;
		public TypeSerializer<V> valueSerializer;
		private Map<Short, Tuple2<TypeSerializer, KeySelector>> selectors;
		private Map<Short, ReduceFunction<V>> reducers;

		public KVOpSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
				Map<Short, ReduceFunction<V>> reducers,
				Map<Short, Tuple2<TypeSerializer, KeySelector>> selectors, ExecutionConfig config) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
			this.reducers = reducers;
			this.selectors = selectors;
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

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			KVOpSerializer<?, ?> that = (KVOpSerializer<?, ?>) o;

			if (!keySerializer.equals(that.keySerializer)) return false;
			if (valueSerializer != null ? !valueSerializer.equals(that.valueSerializer) : that.valueSerializer != null)
				return false;
			if (selectors != null ? !selectors.equals(that.selectors) : that.selectors != null) return false;
			return !(reducers != null ? !reducers.equals(that.reducers) : that.reducers != null);

		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof KVOperationTypeInfo;
		}
		
		@Override
		public int hashCode() {
			int result = keySerializer.hashCode();
			result = 31 * result + (valueSerializer != null ? valueSerializer.hashCode() : 0);
			result = 31 * result + (selectors != null ? selectors.hashCode() : 0);
			result = 31 * result + (reducers != null ? reducers.hashCode() : 0);
			return result;
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
				to.setKey(copyWithReuse(from.getKey(), to.getKey(), keySerializer));
				to.setValue(copyWithReuse(from.getValue(), to.getValue(), valueSerializer));
				break;
			case UPDATE:
				to.setKey(copyWithReuse(from.getKey(), to.getKey(), keySerializer));
				to.setValue(copyWithReuse(from.getValue(), to.getValue(), valueSerializer));
				to.setReducer(from.getReducer());
				break;
			case GET:
			case REMOVE:
				to.setKey(copyWithReuse(from.getKey(), to.getKey(), keySerializer));
				break;
			case MGET:
				to.setKey(copyWithReuse(from.getKey(), to.getKey(), keySerializer));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
				break;
			case MGETRES:
				to.setKey(copyWithReuse(from.getKey(), to.getKey(), keySerializer));
				to.setValue(copyWithReuse(from.getValue(), to.getValue(), valueSerializer));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
				break;
			case SGET:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(copyWithReuse(from.getRecord(), to.getRecord(),
						selectors.get(from.getQueryID()).f0));
				break;
			case SGETRES:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(copyWithReuse(from.getRecord(), to.getRecord(),
						selectors.get(from.getQueryID()).f0));
				to.setValue(copyWithReuse(from.getValue(), to.getValue(), valueSerializer));
				break;
			case SMGET:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(copyWithReuse(from.getRecord(), to.getRecord(),
						selectors.get(from.getQueryID()).f0));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
				break;
			case SMGETRES:
				to.setKeySelector(from.getKeySelector());
				to.setRecord(copyWithReuse(from.getRecord(), to.getRecord(),
						selectors.get(from.getQueryID()).f0));
				to.setValue(copyWithReuse(from.getValue(), to.getValue(), valueSerializer));
				to.setOperationID(from.getOperationID());
				to.setNumKeys(from.getNumKeys());
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

			target.writeShort(op.getQueryID());

			switch (op.getType()) {
			case PUT:
				target.writeByte(0);
				keySerializer.serialize(op.getKey(), target);
				valueSerializer.serialize(op.getValue(), target);
				break;
			case GET:
				target.writeByte(1);
				keySerializer.serialize(op.getKey(), target);
				break;
			case REMOVE:
				target.writeByte(2);
				keySerializer.serialize(op.getKey(), target);
				break;
			case MGET:
				target.writeByte(3);
				keySerializer.serialize(op.getKey(), target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case SGET:
				target.writeByte(4);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				break;
			case GETRES:
				target.writeByte(5);
				keySerializer.serialize(op.getKey(), target);
				serializeValWithNull(op, target);
				break;
			case REMOVERES:
				target.writeByte(6);
				keySerializer.serialize(op.getKey(), target);
				serializeValWithNull(op, target);
				break;
			case MGETRES:
				target.writeByte(7);
				keySerializer.serialize(op.getKey(), target);
				serializeValWithNull(op, target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case SGETRES:
				target.writeByte(8);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				serializeValWithNull(op, target);
				break;
			case SMGET:
				target.writeByte(9);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case SMGETRES:
				target.writeByte(10);
				selectors.get(op.getQueryID()).f0.serialize(op.getRecord(), target);
				serializeValWithNull(op, target);
				target.writeLong(op.getOperationID());
				target.writeShort(op.getNumKeys());
				break;
			case UPDATE:
				target.writeByte(11);
				keySerializer.serialize(op.getKey(), target);
				valueSerializer.serialize(op.getValue(), target);
				break;
			default:
				throw new RuntimeException("Invalid operation: " + op.getType().name());
			}
		}

		private void serializeValWithNull(KVOperation<K, V> op, DataOutputView target) throws IOException {
			boolean hasVal = op.getValue() != null;
			target.writeBoolean(hasVal);
			if (hasVal) {
				valueSerializer.serialize(op.getValue(), target);
			}
		}

		private V deserializeValWithNull(V reuse, DataInputView source) throws IOException {
			if (source.readBoolean()) {
				if (reuse == null) {
					return valueSerializer.deserialize(source);
				} else {
					return valueSerializer.deserialize(reuse, source);
				}
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
			op.setQueryID(source.readShort());

			KVOperationType type = KVOperation.types[source.readByte()];

			op.setType(type);

			switch (type) {
			case PUT:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				op.setValue(deserializeWithReuse(source, op.getValue(), valueSerializer));
				break;
			case GETRES:
			case REMOVERES:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				op.setValue(deserializeValWithNull(op.getValue(), source));
				break;
			case GET:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				break;
			case REMOVE:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				break;
			case MGET:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case SGET:
				Tuple2<TypeSerializer, KeySelector> selector = selectors.get(op.getQueryID());
				op.setKeySelector(selector.f1);
				op.setRecord(deserializeWithReuse(source, op.getRecord(), selector.f0));
				break;
			case MGETRES:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				op.setValue(deserializeValWithNull(op.getValue(), source));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case SGETRES:
				op.setRecord(deserializeWithReuse(source, op.getRecord(), selectors.get(op.getQueryID()).f0));
				op.setValue(deserializeValWithNull(op.getValue(), source));
				break;
			case SMGET:
				Tuple2<TypeSerializer, KeySelector> s = selectors.get(op.getQueryID());
				op.setKeySelector(s.f1);
				op.setRecord(deserializeWithReuse(source, op.getRecord(), s.f0));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case SMGETRES:
				op.setRecord(deserializeWithReuse(source, op.getRecord(), selectors.get(op.getQueryID()).f0));
				op.setValue(deserializeValWithNull(op.getValue(), source));
				op.setOperationID(source.readLong());
				op.setNumKeys(source.readShort());
				break;
			case UPDATE:
				op.setKey(deserializeWithReuse(source, op.getKey(), keySerializer));
				op.setValue(deserializeWithReuse(source, op.getValue(), valueSerializer));
				op.setReducer(reducers.get(op.getQueryID()));
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
		
		public static <X> X copyWithReuse(X from, X reuse, TypeSerializer<X> serializer) {
			if (from == null) {
				return null;
			} else {
				if (reuse == null) {
					return serializer.copy(from);
				} else {
					return serializer.copy(from, reuse);
				}
			}
		}

		private static <X> X deserializeWithReuse(DataInputView source, X reuse, TypeSerializer<X> serializer)
				throws IOException {
			if (reuse == null) {
				return serializer.deserialize(source);
			} else {
				return serializer.deserialize(reuse, source);
			}
		}

	}

}
