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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import streamkv.types.KVTypeInfo.KVSerializer;

public class KVArrayTypeInfo<K, V> extends TypeInformation<Tuple2<K, V>[]> {

	private static final long serialVersionUID = 1L;
	private KVTypeInfo<K, V> kvType;
	private KVSerializer<K, V> kvSerializer = null;

	public KVArrayTypeInfo(KVTypeInfo<K, V> kvType) {
		this.kvType = kvType;
	}

	public KVArrayTypeInfo(KVSerializer<K, V> kvSerializer) {
		this.kvSerializer = kvSerializer;
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
	public Class<Tuple2<K, V>[]> getTypeClass() {
		return null;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}
	
	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof KVArrayTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KVArrayTypeInfo<?, ?> that = (KVArrayTypeInfo<?, ?>) o;

		if (kvType != null ? !kvType.equals(that.kvType) : that.kvType != null) return false;
		return !(kvSerializer != null ? !kvSerializer.equals(that.kvSerializer) : that.kvSerializer != null);

	}

	@Override
	public int hashCode() {
		int result = kvType != null ? kvType.hashCode() : 0;
		result = 31 * result + (kvSerializer != null ? kvSerializer.hashCode() : 0);
		return result;
	}

	@Override
	public TypeSerializer<Tuple2<K, V>[]> createSerializer(ExecutionConfig config) {
		if (kvSerializer == null) {
			return new GenericArraySerializer<>(kvType.getTypeClass(), kvType.createSerializer(config));
		} else {
			Tuple2<K, V> instance = new Tuple2<>();
			@SuppressWarnings("unchecked")
			Class<Tuple2<K, V>> c = (Class<Tuple2<K, V>>) instance.getClass();
			return new GenericArraySerializer<>(c, kvSerializer);
		}
	}

	@Override
	public String toString() {
		return "KVArrayTypeInfo{" +
				"kvType=" + kvType +
				", kvSerializer=" + kvSerializer +
				'}';
	}
}
