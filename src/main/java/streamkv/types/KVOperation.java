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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import streamkv.api.KVStore;

import java.io.Serializable;

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
public class KVOperation<K, V> implements Serializable {

	private static final long serialVersionUID = 4333191409809358657L;
	public enum KVOperationType {
		PUT, GET, REMOVE, MGET, SGET, GETRES, REMOVERES, MGETRES, SGETRES, SMGET, SMGETRES, UPDATE;
	}

	public static KVOperationType[] types = KVOperationType.values();

	// General attributes
	private KVOperationType type;
	private short queryID;

	// For standard PUT/GET operations
	private K key;
	private V value;

	// For GET with KeySelector
	private Object record;
	private KeySelector<Object, K> keySelector;
	
	// For UPDATE operations
	private ReduceFunction<V> reducer;

	// For MULTIGET operations
	private long operationID;
	private short numKeys;

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
		this.operationID = operationID;
	}

	public void setKey(K key) {
		this.key = key;
	}
	
	public ReduceFunction<V> getReducer() {
		return reducer;
	}

	public void setReducer(ReduceFunction<V> reducer) {
		this.reducer = reducer;
	}


	public K getKey() {
		return key;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public V getValue() {
		return value;
	}

	public void setType(KVOperationType type) {
		this.type = type;
	}

	public KVOperationType getType() {
		return type;
	}

	public void setQueryID(short id) {
		this.queryID = id;
	}

	public short getQueryID() {
		return queryID;
	}

	public void setOperationID(long id) {
		this.operationID = id;
	}

	public long getOperationID() {
		return operationID;
	}

	public void setNumKeys(short nk) {
		this.numKeys = nk;
	}

	public short getNumKeys() {
		return numKeys;
	}

	public Object getRecord() {
		return record;
	}

	public void setRecord(Object record) {
		this.record = record;
	}

	public KeySelector<Object, K> getKeySelector() {
		return keySelector;
	}

	public void setKeySelector(KeySelector<Object, K> keySelector) {
		this.keySelector = keySelector;
	}

	public static <K, V> KVOperation<K, V> put(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.PUT);
	}
	
	public static <K, V> KVOperation<K, V> update(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.UPDATE);
	}

	public static <K, V> KVOperation<K, V> getRes(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.GETRES);
	}

	public static <K, V> KVOperation<K, V> removeRes(int id, K key, V value) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.REMOVERES);
	}

	public static <K, V> KVOperation<K, V> get(int id, K key) {
		return new KVOperation<>((short) id, key, null, null, KVOperationType.GET);
	}

	public static <K, V> KVOperation<K, V> remove(int id, K key) {
		return new KVOperation<>((short) id, key, null, null, KVOperationType.REMOVE);
	}

	public static <K, V> KVOperation<K, V> multiGet(int id, K key, short numKeys, long opID) {
		return new KVOperation<>((short) id, key, null, null, KVOperationType.MGET, numKeys, opID);
	}

	public static <K, V> KVOperation<K, V> selectorMultiGet(int id, Object record, short numKeys, long opID) {
		return new KVOperation<>((short) id, null, null, record, KVOperationType.SMGET, numKeys, opID);
	}

	public static <K, V> KVOperation<K, V> multiGetRes(int id, K key, V value, short numKeys, long opID) {
		return new KVOperation<>((short) id, key, value, null, KVOperationType.MGETRES, numKeys, opID);
	}

	public static <K, V> KVOperation<K, V> selectorMultiGetRes(int id, Object record, V value, short numKeys,
			long opID) {
		return new KVOperation<>((short) id, null, value, record, KVOperationType.SMGETRES, numKeys, opID);
	}

	public static <K, V> KVOperation<K, V> selectorGet(int id, Object record) {
		return new KVOperation<>((short) id, null, null, record, KVOperationType.SGET);
	}

	public static <K, V> KVOperation<K, V> selectorGetRes(int id, Object record, V value) {
		return new KVOperation<>((short) id, null, value, record, KVOperationType.SGETRES);
	}

	public String toString() {
		return "(" + queryID + ", " + type.name() + ", " + key + ", " + value + ", " + record + ", "
				+ numKeys + ", " + operationID + ")";
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

		if (type != other.type) {
			return false;
		}

		if (queryID != other.queryID) {
			return false;
		}

		switch (type) {
		case REMOVE:
		case GET:
			return keyEquals(other);
		case PUT:
		case UPDATE:
		case GETRES:
		case REMOVERES:
			return keyEquals(other) && valueEquals(other);
		case MGET:
			return keyEquals(other) && numKeyEquals(other) && opIDEquals(other);
		case MGETRES:
			return keyEquals(other) && valueEquals(other) && numKeyEquals(other) && opIDEquals(other);
		case SGET:
			return recordEquals(other);
		case SGETRES:
			return valueEquals(other) && recordEquals(other);
		case SMGET:
			return recordEquals(other) && numKeyEquals(other) && opIDEquals(other);
		case SMGETRES:
			return recordEquals(other) && valueEquals(other) && numKeyEquals(other) && opIDEquals(other);
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

	private boolean numKeyEquals(KVOperation<?, ?> other) {
		return numKeys == other.numKeys;
	}

	private boolean opIDEquals(KVOperation<?, ?> other) {
		return operationID == other.operationID;
	}
}
