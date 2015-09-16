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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class KVTypeInfo<K, V> extends TypeInformation<Tuple2<K, V>> {

	private static final long serialVersionUID = 1L;
	private TypeInformation<K> keyType;
	private TypeInformation<V> valueType;

	public KVTypeInfo(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public TypeInformation<K> getKeyType() {
		return keyType;
	}

	public TypeInformation<V> getValueType() {
		return valueType;
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

	@SuppressWarnings("unchecked")
	@Override
	public Class<Tuple2<K, V>> getTypeClass() {
		Tuple2<K, V> instance = new Tuple2<>();
		return (Class<Tuple2<K, V>>) instance.getClass();
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<Tuple2<K, V>> createSerializer(ExecutionConfig config) {
		return new KVSerializer<>(keyType.createSerializer(config), valueType.createSerializer(config));
	}

	public static class KVSerializer<K, V> extends TypeSerializer<Tuple2<K, V>> {

		private static final long serialVersionUID = 1L;
		TypeSerializer<K> keySerializer;
		TypeSerializer<V> valueSerializer;

		public KVSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<Tuple2<K, V>> duplicate() {
			return this;
		}

		@Override
		public Tuple2<K, V> createInstance() {
			return new Tuple2<>();
		}

		@Override
		public Tuple2<K, V> copy(Tuple2<K, V> from) {
			return copy(from, createInstance());
		}

		@Override
		public Tuple2<K, V> copy(Tuple2<K, V> from, Tuple2<K, V> reuse) {
			reuse.f0 = copyWithReuse(from.f0, reuse.f0, keySerializer);
			reuse.f1 = copyWithReuse(from.f1, reuse.f1, valueSerializer);
			return reuse;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(Tuple2<K, V> record, DataOutputView target) throws IOException {
			K key = record.f0;
			V value = record.f1;

			target.writeBoolean(key != null);

			if (key != null) {
				keySerializer.serialize(key, target);
			}

			target.writeBoolean(value != null);
			if (value != null) {
				valueSerializer.serialize(value, target);
			}
		}

		@Override
		public Tuple2<K, V> deserialize(DataInputView source) throws IOException {
			return deserialize(createInstance(), source);
		}

		@Override
		public Tuple2<K, V> deserialize(Tuple2<K, V> reuse, DataInputView source) throws IOException {
			reuse.f0 = deserializeWithReuse(source, reuse.f0, keySerializer);
			reuse.f1 = deserializeWithReuse(source, reuse.f1, valueSerializer);
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("Not implemented yet!");
		}
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

	@Override
	public String toString() {
		return this.getClass().getCanonicalName() + "[" + keyType.toString() + "," + valueType.toString()
				+ "]";
	}

}
