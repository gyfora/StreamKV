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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;

import streamkv.api.java.KVStore;

/**
 * Internal class wrapping all operations that can be applied on a
 * {@link KVStore}. It is used to bring all inputs and outputs to a common
 * representation.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public final class KVOperation<K, V> implements Serializable {

	private static final long serialVersionUID = 4333191409809358657L;

	public enum KVOperationType {
		PUT(false), GET(false), REMOVE(false), MGET(false), SGET(false), KVRES(true), MGETRES(true), SGETRES(true), SMGET(
				false), SKVRES(true), UPDATE(false), SUPDATE(false), LOCK(false);

		public boolean isResult;

		KVOperationType(boolean isResult) {
			this.isResult = isResult;
		}

	}

	public static KVOperationType[] types = KVOperationType.values();

	// General attributes
	public KVOperationType type;
	public short queryID;
	public K key;
	public V value;

	// For selector operations
	public Object record;
	public KeySelector<Object, K> keySelector;

	// For UPDATE operations
	public ReduceFunction<V> reducer;

	// For MULTIGET operations
	public boolean hasOpId;
	public long operationID;
	public short numKeys;
	public short index;

	// For locks
	public byte numOpsForKey;

	// For multi-row operations
	public boolean isPartOfTransaction = false;
	public long[] inputOperationIDs;
	public long transactionID;
	public K dependentKey;

	public KVOperation() {
	}

	private KVOperation(short queryID, K key, V value, Object record, KVOperationType type) {
		this.key = key;
		this.value = value;
		this.type = type;
		this.queryID = queryID;
		this.record = record;
	}

	private KVOperation(short queryID, K key, V value, Object record, KVOperationType type, short numKeys,
			long operationID) {
		this(queryID, key, value, record, type);
		this.numKeys = numKeys;
		this.hasOpId = true;
		setOpID(operationID);
	}

	public KVOperation<K, V> setOpID(long id) {
		hasOpId = true;
		operationID = id;
		return this;
	}

	public KVOperation<K, V> setTransactionID(long id) {
		isPartOfTransaction = true;
		transactionID = id;
		return this;
	}

	public KVOperation<K, V> setInputIDs(long[] ids) {
		inputOperationIDs = ids;
		return this;
	}

	public KVOperation<K, V> setDependentKey(K key) {
		dependentKey = key;
		return this;
	}

	public static <K, V> KVOperation<K, V> put(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.PUT);
	}

	public static <K, V> KVOperation<K, V> update(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.UPDATE);
	}

	public static <K, V> KVOperation<K, V> selectorUpdate(int id, K key, Object record) {
		return new KVOperation<>((short) id, key, null, record, KVOperationType.SUPDATE);
	}

	public static <K, V> KVOperation<K, V> kvRes(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.KVRES);
	}

	public static <K, V> KVOperation<K, V> get(int id, K key) {
		return new KVOperation<>((short) id, key, null, null, KVOperationType.GET);
	}

	public static <K, V> KVOperation<K, V> remove(int id, K key) {
		return new KVOperation<>((short) id, key, null, null, KVOperationType.REMOVE);
	}

	public static <K, V> KVOperation<K, V> multiGet(int id, K key, short numKeys, short index, long opID) {
		KVOperation<K, V> op = new KVOperation<>((short) id, key, null, null, KVOperationType.MGET, numKeys, opID);
		op.index = index;
		return op;
	}

	public static <K, V> KVOperation<K, V> selectorMultiGet(int id, Object record, short numKeys, short index, long opID) {
		KVOperation<K, V> op = new KVOperation<>((short) id, null, null, record, KVOperationType.SMGET, numKeys, opID);
		op.index = index;
		return op;
	}

	public static <K, V> KVOperation<K, V> multiGetRes(int id, K key, V value, short numKeys, short index, long opID) {
		KVOperation<K, V> op = new KVOperation<>((short) id, key, value, null, KVOperationType.MGETRES, numKeys, opID);
		op.index = index;
		return op;
	}

	public static <K, V> KVOperation<K, V> selectorMultiGetRes(int id, Object record, V value, short numKeys,
			short index, long opID) {
		KVOperation<K, V> op = new KVOperation<>((short) id, null, value, record, KVOperationType.SKVRES, numKeys, opID);
		op.index = index;
		return op;
	}

	public static <K, V> KVOperation<K, V> selectorGet(int id, Object record) {
		return new KVOperation<>((short) id, null, null, record, KVOperationType.SGET);
	}

	public static <K, V> KVOperation<K, V> skvRes(int id, Object record, V value) {
		return new KVOperation<>((short) id, null, value, record, KVOperationType.SGETRES);
	}

	public static <K, V> KVOperation<K, V> lock(int id, K key, long lockID, int numOps) {
		KVOperation<K, V> op = new KVOperation<>((short) id, key, null, null, KVOperationType.LOCK, (short) 0, lockID);
		op.numOpsForKey = (byte) numOps;
		return op;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("KVOperation [type=");
		builder.append(type);
		builder.append(", queryID=");
		builder.append(queryID);
		builder.append(", key=");
		builder.append(key);
		builder.append(", value=");
		builder.append(value);
		builder.append(", record=");
		builder.append(record);
		builder.append(", operationID=");
		builder.append(operationID);
		builder.append(", numKeys=");
		builder.append(numKeys);
		builder.append(", index=");
		builder.append(index);
		builder.append(", inputOperationIDs=");
		builder.append(Arrays.toString(inputOperationIDs));
		builder.append(", transactionID=");
		builder.append(transactionID);
		builder.append(", numOpsForKey=");
		builder.append(numOpsForKey);
		builder.append(", dependentKey=");
		builder.append(dependentKey);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + numKeys;
		result = prime * result + (int) (operationID ^ (operationID >>> 32));
		result = prime * result + queryID;
		result = prime * result + ((record == null) ? 0 : record.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof KVOperation)) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		KVOperation other = (KVOperation) obj;

		if (type != other.type || queryID != other.queryID) {
			return false;
		}

		if (!(isPartOfTransaction == other.isPartOfTransaction)) {
			return false;
		}

		if (hasOpId && (!other.hasOpId || !opIDEquals(other))) {
			return false;
		}

		if (isPartOfTransaction
				&& (!Arrays.equals(inputOperationIDs, other.inputOperationIDs) || !dependentKeyEquals(other))) {
			return false;
		}

		switch (type) {
		case REMOVE:
		case GET:
			return keyEquals(other);
		case PUT:
		case UPDATE:
		case KVRES:
			return keyEquals(other) && valueEquals(other);
		case MGET:
			return keyEquals(other) && numKeyAndIndexEquals(other) && opIDEquals(other);
		case MGETRES:
			return keyEquals(other) && valueEquals(other) && numKeyAndIndexEquals(other) && opIDEquals(other);
		case SGET:
			return recordEquals(other);
		case SGETRES:
		case SUPDATE:
			return valueEquals(other) && recordEquals(other);
		case SMGET:
			return recordEquals(other) && numKeyAndIndexEquals(other) && opIDEquals(other);
		case SKVRES:
			return recordEquals(other) && valueEquals(other) && numKeyAndIndexEquals(other) && opIDEquals(other);
		case LOCK:
			return keyEquals(other) && numOpsForKey == other.numOpsForKey && operationID == other.operationID;
		default:
			return false;
		}
	}

	private boolean keyEquals(KVOperation<?, ?> other) {
		if (key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!key.equals(other.key)) {
			return false;
		}
		return true;
	}

	private boolean dependentKeyEquals(KVOperation<?, ?> other) {
		if (dependentKey == null) {
			if (other.dependentKey != null) {
				return false;
			}
		} else if (!dependentKey.equals(other.dependentKey)) {
			return false;
		}
		return true;
	}

	private boolean valueEquals(KVOperation<?, ?> other) {
		if (value == null) {
			if (other.value != null) {
				return false;
			}
		} else if (!value.equals(other.value)) {
			return false;
		}
		return true;
	}

	private boolean recordEquals(KVOperation<?, ?> other) {
		if (record == null) {
			if (other.record != null) {
				return false;
			}
		} else if (!record.equals(other.record)) {
			return false;
		}
		return true;
	}

	private boolean numKeyAndIndexEquals(KVOperation<?, ?> other) {
		return numKeys == other.numKeys && index == other.index;
	}

	private boolean opIDEquals(KVOperation<?, ?> other) {
		return operationID == other.operationID;
	}
}
