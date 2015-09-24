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

package streamkv.api.scala

import org.apache.flink.streaming.api.scala.DataStream
import streamkv.api.java.{ KVStore => JavaStore }
import streamkv.api.java.OperationOrdering
import streamkv.api.java.types.KVOperation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.api.common.functions.ReduceFunction
import scala.util.Random
import org.apache.flink.api.java.functions.KeySelector
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import streamkv.api.scala.kvstorebuilder.ScalaStoreBuilder
import streamkv.api.scala.scala.kvstorebuilder.ScalaStoreBuilder
import scala.collection.mutable.MutableList

import _root_.scala.reflect.ClassTag
import _root_.scala.util.Random

trait KVStore[K, V] {
  /**
   * Put the elements from the given tuple into the key-value store.
   */
  def put(stream: DataStream[(K, V)])
  /**
   * Update the current value for the given key-value pair, by reducing the old and new value with the reduce function.
   */
  def update(stream: DataStream[(K, V)])(reducer: (V, V) => V)
  /**
   * Get elements from the KVStore by key.
   */
  def get(stream: DataStream[K]): Query[(K, V)]
  /**
   * Remove elements from the KVStore by key.
   */
  def remove(stream: DataStream[K]): Query[(K, V)]
  /**
   * Get multiple elements at the same type using an array of keys.
   */
  def multiGet(stream: DataStream[Array[K]]): Query[Array[(K, V)]]
  /**
   * Get elements from the KVStore by passing an object and a function that extracts the key from the object.
   */
  def getWithKeySelector[R](stream: DataStream[R])(key: R => K): Query[(R, V)]
  /**
   * Get multiple elements at the same type using an array of objects and a custom key extractor.
   */
  def multiGetWithKeySelector[R](stream: DataStream[Array[R]])(key: R => K): Query[Array[(R, V)]]
  /**
   * Return all the queries that result in an output stream.
   */
  def getQueries: List[Query[_]]
}

class ScalaKVStore[K: TypeInformation: ClassTag, V: TypeInformation: ClassTag](ordering: OperationOrdering) extends KVStore[K, V] {
  val storeBuilder = new ScalaStoreBuilder[K, V](ordering)
  val queries = new MutableList[Query[_]]

  def put(stream: DataStream[(K, V)]) = {
    val qid = storeBuilder.nextID
    val opstream = stream.map(in => KVOperation.put(qid, in._1, in._2))
      .getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    storeBuilder.put(opstream, qid)
  }
  def update(stream: DataStream[(K, V)])(reduceFun: (V, V) => V) = {
    val qid = storeBuilder.nextID
    val opstream = stream.map(in => KVOperation.update(qid, in._1, in._2))
      .getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    val reducer = new ReduceFunction[V] {
      def reduce(v1: V, v2: V) = { reduceFun(v1, v2) }
    }
    storeBuilder.update(opstream, reducer, qid)
  }
  def get(stream: DataStream[K]) = {
    val qid = storeBuilder.nextID
    val opstream = stream.map(in => KVOperation.get(qid, in))
      .getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    storeBuilder.get(opstream, qid)
    val q = new Query[(K, V)](qid, storeBuilder)
    queries += q
    q
  }
  def remove(stream: DataStream[K]) = {
    val qid = storeBuilder.nextID
    val opstream = stream.map(in => KVOperation.remove(qid, in))
      .getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    storeBuilder.remove(opstream, qid)
    val q = new Query[(K, V)](qid, storeBuilder)
    queries += q
    q
  }
  def multiGet(stream: DataStream[Array[K]]) = {
    val qid = storeBuilder.nextID
    val opstream = stream.flatMap(in => {
      val opID = Random.nextLong
      val numKeys: Short = in.length.toShort
      for (i <- 0 to numKeys - 1) yield KVOperation.multiGet(qid, in(i), numKeys, opID)
    }).getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    storeBuilder.multiGet(opstream, qid)
    val q = new Query[Array[(K, V)]](qid, storeBuilder)
    queries += q
    q
  }
  def getWithKeySelector[R](stream: DataStream[R])(key: R => K) = {
    val qid = storeBuilder.nextID
    val opstream = stream.map(in => KVOperation.selectorGet(qid, in))
      .getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    val selector = new KeySelector[R, K] {
      def getKey(o: R) = key(o)
    }
    storeBuilder.selectorGet(opstream, selector, qid)
    val q = new Query[(R, V)](qid, storeBuilder)
    queries += q
    q
  }

  def multiGetWithKeySelector[R](stream: DataStream[Array[R]])(key: R => K) = {
    val qid = storeBuilder.nextID
    val opstream = stream.flatMap(in => {
      val opID = Random.nextLong
      val numKeys: Short = in.length.toShort
      for (i <- 0 to numKeys - 1) yield KVOperation.selectorMultiGet(qid, in(i), numKeys, opID)
    }).getJavaStream.asInstanceOf[SingleOutputStreamOperator[KVOperation[K, V], _]]
    val selector = new KeySelector[R, K] {
      def getKey(o: R) = key(o)
    }
    storeBuilder.selectorMultiGet(opstream, selector, qid)
    val q = new Query[Array[(R, V)]](qid, storeBuilder)
    queries += q
    q
  }

  def getQueries: List[Query[_]] = queries.toList
}

object KVStore {
  def apply[K: TypeInformation: ClassTag, V: TypeInformation: ClassTag](ordering: OperationOrdering): KVStore[K, V] = new ScalaKVStore[K, V](ordering)
}