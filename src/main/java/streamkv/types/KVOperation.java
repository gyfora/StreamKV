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

import java.io.Serializable;

import org.apache.flink.api.java.functions.KeySelector;

import streamkv.api.KVStore;

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
		PUT, GET, REMOVE, MGET, SGET, GETRES, REMOVERES, MGETRES, SGETRES;
	}

	public static KVOperationType[] types = KVOperationType.values();

	// General attributes
	private KVOperationType type;
	private int queryID;

	// For standard PUT/GET operations
	private K key;
	private V value;

	// For GET with extractor
	private Object record;
	private KeySelector<Object, K> keySelector;

	// For MULTIGET operations
	private long operationID;
	private short numKeys;

	public KVOperation() {
	}

	// For PUT/GETRES/REMOVERES
	public KVOperation(K key, V value, int queryID, KVOperationType type) {
		this.key = key;
		this.value = value;
		this.type = type;
		this.queryID = queryID;
	}

	public KVOperation(K key, short numKeys, long operationID, int queryID) {
		this.key = key;
		this.type = KVOperationType.MGET;
		this.queryID = queryID;
		this.numKeys = numKeys;
		this.operationID = operationID;
	}

	public KVOperation(K key, V value, short numKeys, long operationID, int queryID) {
		this.key = key;
		this.value = value;
		this.type = KVOperationType.MGETRES;
		this.queryID = queryID;
		this.numKeys = numKeys;
		this.operationID = operationID;
	}

	// For GET/REMOVE
	public KVOperation(K key, int queryID, KVOperationType type) {
		this(key, null, queryID, type);
		this.type = type;
	}

	// For XGET
	public KVOperation(Object record, int queryID) {
		this.queryID = queryID;
		this.record = record;
		this.type = KVOperationType.SGET;
	}

	// For XGETRES
	public KVOperation(Object record, V value, int queryID) {
		this.queryID = queryID;
		this.record = record;
		this.value = value;
		this.type = KVOperationType.SGETRES;
	}

	public void setKey(K key) {
		this.key = key;
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

	public void setQueryID(int id) {
		this.queryID = id;
	}

	public int getQueryID() {
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

	public String toString() {
		return type.name() + "\nKey: " + key + "\nValue: " + value + "\nNumKeys: " + numKeys;
	}
}
