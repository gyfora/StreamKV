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

public class KVArrayTypeInfo<K, V> extends TypeInformation<Tuple2<K, V>[]> {

	private static final long serialVersionUID = 1L;
	private KVTypeInfo<K, V> kvType;

	public KVArrayTypeInfo(KVTypeInfo<K, V> kvType) {
		this.kvType = kvType;
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
	public TypeSerializer<Tuple2<K, V>[]> createSerializer(ExecutionConfig config) {
		return new GenericArraySerializer<>(kvType.getTypeClass(), kvType.createSerializer(config));
	}

}
