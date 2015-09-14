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

package streamkv.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * MODIFICATION OF KV CLASS FROM GOOGLE CLOUD DATAFLOW JAVA SDK
 * 
 * THE APACHE 2.0 LICENSED CODE REPOSITORY IS FOUND AT
 * https://github.com/GoogleCloudPlatform/DataflowJavaSDK
 * 
 * A key/value pair. This implementation is needed instead of basing it on
 * Tuple, because the TupleSerializer currently does not handle null values.
 * 
 * @param <K>
 *            the type of the key
 * @param <V>
 *            the type of the value
 */
public class KV<K, V> implements Serializable {
	private static final long serialVersionUID = 0;

	/** Returns a KV with the given key and value. */
	public static <K, V> KV<K, V> of(K key, V value) {
		return new KV<>(key, value);
	}

	/** Returns the key of this KV. */
	public K getKey() {
		return key;
	}

	/** Returns the value of this KV. */
	public V getValue() {
		return value;
	}

	/** Sets the key of this KV. */
	public void setKey(K key) {
		this.key = key;
	}

	/** Sets the value of this KV. */
	public void setValue(V value) {
		this.value = value;
	}

	// ///////////////////////////////////////////////////////////////////////////

	private K key;
	private V value;

	private KV(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public KV() {
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof KV)) {
			return false;
		}
		KV<?, ?> otherKv = (KV<?, ?>) other;
		// Arrays are very common as values and keys, so deepEquals is mandatory
		return Objects.deepEquals(this.key, otherKv.key) && Objects.deepEquals(this.value, otherKv.value);
	}

	@Override
	public int hashCode() {
		// Objects.deepEquals requires Arrays.deepHashCode for correctness
		return Arrays.deepHashCode(new Object[] { key, value });
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).addValue(key).addValue(value).toString();
	}
}
