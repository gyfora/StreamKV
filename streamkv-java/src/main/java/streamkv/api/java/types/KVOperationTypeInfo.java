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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public final class KVOperationTypeInfo<K, V> extends TypeInformation<KVOperation<K, V>> {

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
		return new KVOperationSerializer<>(keyType.createSerializer(config), valueType == null ? null
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
		return "KVOperationTypeInfo{" + "keyType=" + keyType + ", valueType=" + valueType + ", selectors=" + selectors
				+ ", reducers=" + reducers + '}';
	}

}
