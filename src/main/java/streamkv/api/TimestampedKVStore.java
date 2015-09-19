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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import streamkv.operator.TimestampedKVStoreOperator;
import streamkv.types.KVOperation;
import streamkv.types.KVOperationTypeInfo.KVOpSerializer;

/**
 * {@link KVStore} implementation that executes operations in time order. Time
 * can be ingress time by default or custom event timestamps and watermarks can
 * be provided by the source implementations. There is no ordering guarantee
 * among elements bearing the same timestamps.
 * 
 * <p>
 * This implementation provides deterministic processing guarantees given that
 * each record has a unique timestamp.
 * </p>
 * 
 * <p>
 * Record timestamps need to be enabled by calling
 * {@link ExecutionConfig#enableTimestamps()}.
 * </p>
 * 
 * @see {@link EventTimeSourceFunction}
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public class TimestampedKVStore<K, V> extends AsyncKVStore<K, V> {

	protected TimestampedKVStore() {
	}
	
	@Override
	protected OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> getKVOperator(KVOpSerializer<K, V> serializer) {
		return new TimestampedKVStoreOperator<>(serializer);
	}
}
