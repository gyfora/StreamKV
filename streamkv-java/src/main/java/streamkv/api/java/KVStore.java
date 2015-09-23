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

package streamkv.api.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;

import streamkv.api.java.kvstorebuilder.JavaKVStoreBuilder;
import streamkv.api.java.kvstorebuilder.AbstractKVStoreBuilder;
import streamkv.api.java.util.KVUtils;

/**
 * Basic streaming key-value store abstraction. The user can use Flink
 * {@link DataStream}s to apply operations on the store, such as {@link #put} or
 * {@link #get}. Operations that generate output return a query id that can be
 * used to retrieve the result streams from the {@link KVStoreOutput} instance
 * after calling {@link #getOutputs()}.
 * 
 * <p>
 * Calling {@link KVStore#getOutputs()} will finalize the operations applied to
 * the store, and no further operations can be applied after that in order to
 * avoid creating circular dependencies in the resulting program. If such logic
 * is necessary, it needs to be handled manually using
 * {@link IterativeDataStream}.
 * </p>
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public class KVStore<K, V> {

	private final AbstractKVStoreBuilder<K, V> storeBuilder;
	private List<Query<?>> queries = new ArrayList<>();

	private KVStore(OperationOrdering ordering) {
		this.storeBuilder = new JavaKVStoreBuilder<>(ordering);
	}

	/**
	 * Put the elements from the given tuple stream into the key-value store.
	 * 
	 * @param stream
	 *            Stream of {@link Tuple2}s representing the (K,V) pairs.
	 */
	public void put(DataStream<Tuple2<K, V>> stream) {
		int qid = storeBuilder.nextID();
		storeBuilder.put(stream.map(new KVUtils.ToPut<K, V>(qid)), qid);
	}

	/**
	 * Update the value of the provided key by reducing it with the current
	 * value using the reduce function provided. If there is no entry update
	 * this works like a put operation and creates a new KV entry for the given
	 * key.
	 * 
	 * @param stream
	 *            Stream of {@link Tuple2}s representing the (K,V) pairs.
	 * @param reducer
	 *            The reducer used to update the elements.
	 */
	public void update(DataStream<Tuple2<K, V>> stream, ReduceFunction<V> reducer) {
		int qid = storeBuilder.nextID();
		storeBuilder.update(stream.map(new KVUtils.ToUpdate<K, V>(qid)), reducer, qid);
	}

	/**
	 * Get elements from the store by specifying a stream of keys to retrieve.
	 * 
	 * @param stream
	 *            The stream of keys to get.
	 * @return The resulting {@link Query}.
	 */
	public Query<Tuple2<K, V>> get(DataStream<K> stream) {
		int qid = storeBuilder.nextID();
		storeBuilder.get(stream.map(new KVUtils.ToGet<K, V>(qid)), qid);
		Query<Tuple2<K, V>> q = new Query<Tuple2<K, V>>(qid, storeBuilder);
		queries.add(q);
		return q;
	}

	/**
	 * Remove elements from the store by specifying a stream of keys to remove.
	 * 
	 * @param stream
	 *            The stream of keys to remove.
	 * @return The resulting {@link Query}.
	 */
	public Query<Tuple2<K, V>> remove(DataStream<K> stream) {
		int qid = storeBuilder.nextID();
		storeBuilder.remove(stream.map(new KVUtils.ToRemove<K, V>(qid)), qid);
		Query<Tuple2<K, V>> q = new Query<Tuple2<K, V>>(qid, storeBuilder);
		queries.add(q);
		return q;
	}

	/**
	 * Get elements from the store by specifying a stream of records and a
	 * {@link KeySelector} for extracting the key from each record.
	 * 
	 * @param stream
	 *            The stream of records for which the key will be extracted.
	 * @param keySelector
	 *            The {@link KeySelector} used to extract the key for each
	 *            element.
	 * @return The resulting {@link Query}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <X> Query<Tuple2<X, V>> getWithKeySelector(DataStream<X> stream, KeySelector<X, K> keySelector) {
		int qid = storeBuilder.nextID();
		storeBuilder.selectorGet(((DataStream) stream).map(new KVUtils.ToSGet<K, V>(qid)), keySelector, qid);
		Query<Tuple2<X, V>> q = new Query<Tuple2<X, V>>(qid, storeBuilder);
		queries.add(q);
		return q;
	}

	/**
	 * Get multiple elements from the store at the same time by specifying a
	 * stream of key arrays to retrieve.
	 * 
	 * @param stream
	 *            The stream of key arrays to get.
	 * @return The resulting {@link Query}.
	 */
	public Query<Tuple2<K, V>[]> multiGet(DataStream<K[]> stream) {
		int qid = storeBuilder.nextID();
		storeBuilder.multiGet(stream.flatMap(new KVUtils.ToMGet<K, V>(qid)), qid);
		Query<Tuple2<K, V>[]> q = new Query<Tuple2<K, V>[]>(qid, storeBuilder);
		queries.add(q);
		return q;
	}

	/**
	 * Get multiple elements from the store at the same time by specifying a
	 * stream of object arrays and a {@link KeySelector} for extracting the keys
	 * from the objects.
	 * 
	 * @param stream
	 *            The stream of object arrays.
	 * @param keySelector
	 *            The {@link KeySelector} used to extract the key for each
	 *            element.
	 * @return The resulting {@link Query}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <X> Query<Tuple2<X, V>[]> multiGetWithKeySelector(DataStream<X[]> stream,
			KeySelector<X, K> keySelector) {
		int qid = storeBuilder.nextID();
		storeBuilder.selectorMultiGet(((DataStream) stream).flatMap(new KVUtils.ToSMGet<K, V>(qid)),
				keySelector, qid);
		Query<Tuple2<X, V>[]> q = new Query<Tuple2<X, V>[]>(qid, storeBuilder);
		queries.add(q);
		return q;
	}

	/**
	 * Returns a list of all {@link Query}s applied on this store.
	 * 
	 */
	public List<Query<?>> getQueries() {
		return queries;
	}

	/**
	 * Creates a new {@link KVStore} with the given {@link OperationOrdering}
	 * semantics. <br>
	 * <br>
	 * Currently there are 2 supported ordering semantics:
	 * <ul>
	 * <li><b>ARRIVALTIME</b> : All operations are executed in arrival order
	 * (governed by the standard Flink partial ordering guarantees). While this
	 * implementation provides maximal performance it does not provide any
	 * deterministic processing guarantee.</li>
	 * <li><b>TIMESTAMP</b> : All operations are executed in time order. Time can be
	 * ingress time by default or custom event timestamps and watermarks must be
	 * provided by the source implementations. There is no ordering guarantee
	 * among elements with the same timestamps.
	 * 
	 * <p>
	 * This implementation provides deterministic processing guarantees given
	 * that each record has a unique timestamp.
	 * </p>
	 * 
	 * <p>
	 * Record timestamps need to be enabled by calling
	 * {@link ExecutionConfig#enableTimestamps()}.
	 * </p>
	 * </li>
	 * </ul>
	 * 
	 * @param ordering
	 *            {@link OperationOrdering} semantics for the {@link KVStore}.
	 * @return A new {@link KVStore} instance.
	 */
	public static <K, V> KVStore<K, V> withOrdering(OperationOrdering ordering) {
		return new KVStore<K, V>(ordering);
	}
}
