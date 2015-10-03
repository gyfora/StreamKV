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

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class NullHandlerSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	private TypeSerializer<T> serializer;

	public NullHandlerSerializer(TypeSerializer<T> serializer) {
		super();
		this.serializer = serializer;
	}

	@Override
	public boolean isImmutableType() {
		return serializer.isImmutableType();
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return new NullHandlerSerializer<>(serializer.duplicate());
	}

	@Override
	public T createInstance() {
		return serializer.createInstance();
	}

	@Override
	public T copy(T from) {
		if (from == null) {
			return null;
		} else {
			return serializer.copy(from);
		}
	}

	@Override
	public T copy(T from, T reuse) {
		if (from == null) {
			return null;
		} else if (reuse == null) {
			return serializer.copy(from);
		} else {
			return serializer.copy(from, reuse);
		}
	}

	@Override
	public int getLength() {
		return serializer.getLength();
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		if (record == null) {
			target.writeBoolean(false);
		} else {
			target.writeBoolean(true);
			serializer.serialize(record, target);
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		if (source.readBoolean()) {
			return serializer.deserialize(source);
		} else {
			return null;
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		if (source.readBoolean()) {
			if (reuse != null) {
				return serializer.deserialize(reuse, source);
			} else {
				return serializer.deserialize(source);
			}
		} else {
			return null;
		}
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		boolean notNull = source.readBoolean();
		target.writeBoolean(notNull);
		if (notNull) {
			serializer.copy(source, target);
		}
	}

	@Override
	public boolean equals(Object obj) {
		return serializer.equals(obj);
	}

	@Override
	public boolean canEqual(Object obj) {
		return serializer.canEqual(obj);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}

}
