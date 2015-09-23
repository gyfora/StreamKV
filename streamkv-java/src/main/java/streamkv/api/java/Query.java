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

import org.apache.flink.streaming.api.datastream.DataStream;

import streamkv.api.java.kvstorebuilder.AbstractKVStoreBuilder;

/**
 * Contains the result stream of a {@link KVStore} query.
 * 
 * @param <T>
 *            The type of the output {@link DataStream}.
 */
public class Query<T> {

	private int id;
	private AbstractKVStoreBuilder<?, ?> storebuilder;

	public Query(int id, AbstractKVStoreBuilder<?, ?> storebuilder) {
		super();
		this.id = id;
		this.storebuilder = storebuilder;
	}

	/**
	 * Returns the output stream of this {@link Query}.
	 * 
	 * <p>
	 * Once the output has been retrieved from one of the queries of a given
	 * {@link KVStore}, no more queries can be applied.
	 * </p>
	 */
	@SuppressWarnings("unchecked")
	public DataStream<T> getOutput() {
		return storebuilder.getOutputs().get(id);
	}
}
