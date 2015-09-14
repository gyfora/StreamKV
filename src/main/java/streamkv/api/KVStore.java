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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;

/**
 * Basic streaming key-value store interface. The user can use Flink
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
public interface KVStore<K, V> {

	/**
	 * Put the elements in the given stream into the key-value store.
	 * 
	 * @param stream
	 *            Stream of {@link Tuple2}s representing the (K,V) pairs.
	 */
	void put(DataStream<Tuple2<K, V>> stream);

	/**
	 * Get elements from the store by specifying a stream of keys to retrieve.
	 * 
	 * @param stream
	 *            The stream of keys to get.
	 * @return The id of the resulting (key, value) stream.
	 */
	int get(DataStream<K> stream);

	/**
	 * Remove elements from the store by specifying a stream of keys to remove.
	 * 
	 * @param stream
	 *            The stream of keys to remove.
	 * @return The id of the resulting (key, value) stream.
	 */
	int remove(DataStream<K> stream);

	/**
	 * Get elements from the store by specifying a stream of records and a
	 * {@link KeySelector} for extracting the key from each record.
	 * 
	 * @param stream
	 *            The stream of records for which the key will be extracted.
	 * @param keySelector
	 *            The {@link KeySelector} used to extract the key for each
	 *            element.
	 * @return The id of the resulting (record, value) stream.
	 */
	<X> int getWithKeySelector(DataStream<X> stream, KeySelector<X, K> keySelector);

	/**
	 * Get multiple elements from the store at the same time by specifying a
	 * stream of key arrays to retrieve.
	 * 
	 * @param stream
	 *            The stream of key arrays to get.
	 * @return The id of the resulting (key, value) array stream.
	 */
	int multiGet(DataStream<K[]> stream);

	/**
	 * Finalize the operations applied on this {@link KVStore} and get the
	 * resulting streams. Each result stream can be retrieved from the returned
	 * {@link KVStoreOutput} by calling the respective methods.
	 * 
	 * <p>
	 * After calling this method, no further operations can be applied on the
	 * {@link KVStore} in order to avoid creating circular dependencies in the
	 * resulting program. If such logic is necessary, it needs to be handled
	 * manually using {@link IterativeDataStream}.
	 * 
	 * @return The {@link KVStoreOutput} for this store.
	 */
	KVStoreOutput<K, V> getOutputs();

}