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

package api.scala

import streamkv.api.java.kvstorebuilder.AbstractKVStoreBuilder
import org.apache.flink.streaming.api.datastream.{ DataStream => JavaStream }
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Contains the result stream of a KVStore query
 */
class Query[T](qid: Int, storebuilder: AbstractKVStoreBuilder[_, _]) {

  /**
   * Return the output stream of this query. Once the outputs has been retrieved
   * from one of the queries of a given KVStore, no more queries can be applied.
   */
  def getOutput: DataStream[T] = new DataStream[T](storebuilder.getOutputs().get(qid).asInstanceOf[JavaStream[T]])

}