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

package streamkv.api.java.kvstorebuilder;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import streamkv.api.java.OperationOrdering;
import streamkv.api.java.operator.MultiGetMerger;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationTypeInfo;
import streamkv.api.java.util.KVUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class JavaKVStoreBuilder<K, V> extends AbstractKVStoreBuilder<K, V> {

	public JavaKVStoreBuilder(OperationOrdering ordering) {
		super(ordering);
	}

	private KVOperationTypeInfo<K, V> kvOperationType = null;

	@Override
	public KVOperationTypeInfo<K, V> getKVOperationType() {

		TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) getInKVType();

		if (kvOperationType != null) {
			return kvOperationType;
		} else {
			kvOperationType = (KVOperationTypeInfo<K, V>) new KVOperationTypeInfo<>(tupleType.getTypeAt(0),
					tupleType.getTypeAt(1));
			return kvOperationType;
		}
	}

	@Override
	public DataStream toKVStream(DataStream<KVOperation<K, V>> stream) {
		return KVUtils.nonCopyingMap(stream, getKVType(), new KVUtils.ToKV<K, V>());
	}

	@Override
	public DataStream toSKVStream(DataStream<KVOperation<K, V>> stream, TypeInformation recordType) {
		return KVUtils.nonCopyingMap(stream, KVUtils.getKVType(recordType, getKVOperationType().valueType),
				new KVUtils.ToSKV<K, V>());
	}

	private TupleTypeInfo<Tuple2<K, V>> getKVType() {
		KVOperationTypeInfo<K, V> kvOperationType = getKVOperationType();
		return (TupleTypeInfo<Tuple2<K, V>>) KVUtils.getKVType(kvOperationType.keyType,
				kvOperationType.valueType);
	}

	@Override
	public DataStream toKVArrayStream(DataStream<KVOperation<K, V>> stream) {
		return stream.flatMap(
				new MultiGetMerger<K, V>(getKVOperationType().keyType.createSerializer(getConfig()),
						getKVOperationType().valueType.createSerializer(getConfig()))).returns(
				(TypeInformation) ObjectArrayTypeInfo.getInfoFor(getKVType()));
	}

	@Override
	public DataStream toSKVArrayStream(DataStream<KVOperation<K, V>> stream, TypeInformation recordType) {

		ExecutionConfig config = getConfig();
		TypeSerializer valueSerializer = getKVOperationType().valueType.createSerializer(config);
		TypeSerializer componentSerializer = getComponentType(recordType).createSerializer(config);

		return stream.flatMap(new MultiGetMerger<K, V>(componentSerializer, valueSerializer)).returns(
				ObjectArrayTypeInfo.getInfoFor(KVUtils.getKVType(getComponentType(recordType), getKVType()
						.getTypeAt(1))));
	}
}
