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

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * The output of a {@link KVStore} containing all the output streams resulting
 * from the applied operations. Each output stream can be accessed by calling
 * the respective methods:
 * <ul>
 * <li>{@link KVStore#get}, {@link KVStore#remove} => {@link #getKVStream(int)}</li>
 * <li>{@link KVStore#getWithKeySelector} => {@link #getCustomKVStream(int)}</li>
 * <li>{@link KVStore#multiGet} => {@link #getKVArrayStream(int)}</li>
 * </ul>
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
@SuppressWarnings("rawtypes")
public class KVStoreOutput<K, V> {

	private Map<Integer, DataStream> customKeyValueStreams;
	private Map<Integer, DataStream<Tuple2<K, V>[]>> keyValueArrayStreams;

	public KVStoreOutput(Map<Integer, DataStream> customKeyValueStreams,
			Map<Integer, DataStream<Tuple2<K, V>[]>> keyValueArrayStreams) {
		this.customKeyValueStreams = customKeyValueStreams;
		this.keyValueArrayStreams = keyValueArrayStreams;
	}

	/**
	 * Get the result stream for {@link KVStore#get}, {@link KVStore#remove} and
	 * {@link KVStore#getWithKeySelector} operations.
	 * <p>
	 * Use type hints to aid the compiler:<br>
	 * output.&lt;K,V&gt;getKVStream(queryID)
	 * 
	 * @param queryID
	 * @return The resulting (key, value) stream.
	 */
	@SuppressWarnings("unchecked")
	public <X> DataStream<Tuple2<X, V>> getKVStream(int queryID) {
		if (customKeyValueStreams.containsKey(queryID)) {
			return customKeyValueStreams.get(queryID);
		} else {
			throw new IllegalArgumentException("Given query ID does not correspond to a KV stream.");
		}
	}

	/**
	 * Get the result stream for a {@link KVStore#multiGet} operation.
	 * 
	 * @param queryID
	 * @return The resulting (key, value) array stream.
	 */
	public <X> DataStream<Tuple2<K, V>[]> getKVArrayStream(int queryID) {
		if (keyValueArrayStreams.containsKey(queryID)) {
			return keyValueArrayStreams.get(queryID);
		} else {
			throw new IllegalArgumentException("Given query ID does not correspond to a multi-KV stream.");
		}
	}

}