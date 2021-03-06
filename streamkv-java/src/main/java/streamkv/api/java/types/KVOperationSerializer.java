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

package streamkv.api.java.types;

import static streamkv.api.java.types.KVOperation.KVOperationType.UPDATE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import streamkv.api.java.types.KVOperation.KVOperationType;

@SuppressWarnings("rawtypes")
public final class KVOperationSerializer<K, V> extends TypeSerializer<KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;
	public TypeSerializer<K> keySerializer;
	public TypeSerializer<V> valueSerializer;
	private Map<Short, Tuple2<TypeSerializer, KeySelector>> selectors;
	private Map<Short, ReduceFunction<V>> reducers;

	public KVOperationSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
			Map<Short, ReduceFunction<V>> reducers, Map<Short, Tuple2<TypeSerializer, KeySelector>> selectors,
			ExecutionConfig config) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.reducers = reducers;
		this.selectors = selectors;
	}

	@SuppressWarnings("unchecked")
	@Override
	public KVOperation<K, V> copy(KVOperation<K, V> from, KVOperation<K, V> to) {
		KVOperationType type = from.type;

		to.type = from.type;
		to.queryID = from.queryID;
		to.isPartOfTransaction = from.isPartOfTransaction;
		if (to.isPartOfTransaction) {
			to.transactionID = from.transactionID;
			to.dependentKey = copyWithReuse(from.dependentKey, to.dependentKey, keySerializer);
			to.inputOperationIDs = Arrays.copyOf(from.inputOperationIDs, from.inputOperationIDs.length);
		}
		to.hasOpId = from.hasOpId;
		to.operationID = from.operationID;
		to.numOpsForKey = from.numOpsForKey;

		// copy key/record
		switch (type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case GET:
		case REMOVE:
		case MGET:
		case LOCK:
			to.key = copyWithReuse(from.key, to.key, keySerializer);
			break;
		case SGET:
		case SMGET:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			to.record = copyWithReuse(from.record, to.record, selectors.get(from.queryID).f0);
			break;
		default:
			throw new UnsupportedOperationException();
		}

		// copy value
		switch (type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			to.value = copyWithReuse(from.value, to.value, valueSerializer);
			break;
		default:
			break;
		}

		// copy multiget info
		switch (type) {
		case MGET:
		case MGETRES:
		case SMGET:
		case SKVRES:
			to.numKeys = from.numKeys;
			to.index = from.index;
			break;
		default:
			break;
		}

		to.keySelector = from.keySelector;
		to.reducer = from.reducer;

		return to;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void serialize(KVOperation<K, V> op, DataOutputView target) throws IOException {

		target.writeByte(indexOf(KVOperation.types, op.type));
		target.writeShort(op.queryID);

		target.writeBoolean(op.hasOpId);
		if (op.hasOpId) {
			target.writeLong(op.operationID);
		}

		target.writeBoolean(op.isPartOfTransaction);
		if (op.isPartOfTransaction) {
			target.writeLong(op.transactionID);
			serializeWithNull(op.dependentKey, keySerializer, target);
			target.writeByte(op.inputOperationIDs.length);
			for (long id : op.inputOperationIDs) {
				target.writeLong(id);
			}
		}

		// copy key/record
		switch (op.type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case GET:
		case REMOVE:
		case MGET:
		case LOCK:
			keySerializer.serialize(op.key, target);
			break;
		case SGET:
		case SMGET:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			selectors.get(op.queryID).f0.serialize(op.record, target);
			break;
		default:
			break;
		}

		// copy value
		switch (op.type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			serializeWithNull(op.value, valueSerializer, target);
			break;
		default:
			break;
		}

		// copy multiget info
		switch (op.type) {
		case MGET:
		case MGETRES:
		case SMGET:
		case SKVRES:
			target.writeShort(op.numKeys);
			target.writeShort(op.index);
			break;
		default:
			break;
		}

		if (op.type == KVOperationType.LOCK) {
			target.writeByte(op.numOpsForKey);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public KVOperation<K, V> deserialize(KVOperation<K, V> op, DataInputView source) throws IOException {

		op.type = KVOperation.types[source.readByte()];
		op.queryID = source.readShort();
		if (source.readBoolean()) {
			op.hasOpId = true;
			op.operationID = source.readLong();
		} else {
			op.hasOpId = false;
		}
		if (source.readBoolean()) {
			op.isPartOfTransaction = true;
			op.transactionID = source.readLong();
			op.dependentKey = deserializeWithNull(op.dependentKey, keySerializer, source);
			op.inputOperationIDs = new long[source.readByte()];
			for (int i = 0; i < op.inputOperationIDs.length; i++) {
				op.inputOperationIDs[i] = source.readLong();
			}
		} else {
			op.isPartOfTransaction = false;
		}

		// copy key/record
		switch (op.type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case GET:
		case REMOVE:
		case MGET:
		case LOCK:
			op.key = deserializeWithReuse(source, op.key, keySerializer);
			break;
		case SGET:
		case SMGET:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			Tuple2<TypeSerializer, KeySelector> selector = selectors.get(op.queryID);
			op.keySelector = selector.f1;
			op.record = deserializeWithReuse(source, op.record, selector.f0);
			break;
		default:
			break;
		}

		// copy value
		switch (op.type) {
		case PUT:
		case KVRES:
		case UPDATE:
		case MGETRES:
		case SGETRES:
		case SKVRES:
		case SUPDATE:
			op.value = deserializeWithNull(op.value, valueSerializer, source);
			break;
		default:
			break;
		}

		// copy multiget info
		switch (op.type) {
		case MGET:
		case MGETRES:
		case SMGET:
		case SKVRES:
			op.numKeys = source.readShort();
			op.index = source.readShort();
			break;
		default:
			break;
		}

		if (op.type == KVOperationType.LOCK) {
			op.numOpsForKey = source.readByte();
		}

		if (op.type == UPDATE || op.type == KVOperationType.SUPDATE) {
			op.reducer = reducers.get(op.queryID);
		}

		return op;
	}

	private <X> void serializeWithNull(X val, TypeSerializer<X> serializer, DataOutputView target) throws IOException {
		boolean hasVal = val != null;
		target.writeBoolean(hasVal);
		if (hasVal) {
			serializer.serialize(val, target);
		}
	}

	private <X> X deserializeWithNull(X reuse, TypeSerializer<X> serializer, DataInputView source) throws IOException {
		if (source.readBoolean()) {
			if (reuse == null) {
				return serializer.deserialize(source);
			} else {
				return serializer.deserialize(reuse, source);
			}
		} else {
			return null;
		}
	}

	private static <X> X copyWithReuse(X from, X reuse, TypeSerializer<X> serializer) {
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

	private static <X> int indexOf(X[] arr, X val) {
		int c = 0;
		for (X element : arr) {
			if (element.equals(val)) {
				return c;
			} else {
				c++;
			}
		}
		return -1;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public KVOperation<K, V> deserialize(DataInputView source) throws IOException {
		return deserialize(createInstance(), source);
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
	public int getLength() {
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KVOperationSerializer<?, ?> that = (KVOperationSerializer<?, ?>) o;

		if (!keySerializer.equals(that.keySerializer)) {
			return false;
		}
		if (valueSerializer != null ? !valueSerializer.equals(that.valueSerializer) : that.valueSerializer != null) {
			return false;
		}
		if (selectors != null ? !selectors.equals(that.selectors) : that.selectors != null) {
			return false;
		}
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

}
