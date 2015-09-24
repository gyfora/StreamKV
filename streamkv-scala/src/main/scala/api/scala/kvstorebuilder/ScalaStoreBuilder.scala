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

package api.scala.kvstorebuilder

import scala.Array.canBuildFrom
import scala.collection.immutable.List.apply
import scala.reflect.ClassTag
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.typeutils.CaseClassSerializer
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala._
import streamkv.api.java.OperationOrdering
import streamkv.api.java.kvstorebuilder.AbstractKVStoreBuilder
import streamkv.api.java.operator.MultiGetMerger
import streamkv.api.java.types._
import streamkv.api.java.util.KVUtils
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo

class ScalaStoreBuilder[K: TypeInformation: ClassTag, V: TypeInformation: ClassTag](ordering: OperationOrdering) extends AbstractKVStoreBuilder[K, V](ordering) {

  val kvType = implicitly[TypeInformation[(K, V)]]
  var kvOpType: KVOperationTypeInfo[K, V] = null

  def toKVStream(stream: DataStream[KVOperation[K, V]]): DataStream[_] = {
    KVUtils.nonCopyingMap(stream, createCaseClassTypeInfo(kvOpType.keyType, kvOpType.valueType)
      .asInstanceOf[TypeInformation[(K, V)]],
      new MapFunction[KVOperation[K, V], (K, V)]() {
        def map(in: KVOperation[K, V]) = (in.getKey(), in.getValue())
      })
  }

  def toSKVStream(stream: DataStream[KVOperation[K, V]], recordType: TypeInformation[_]): DataStream[_] = {
    KVUtils.nonCopyingMap(stream, createCaseClassTypeInfo(recordType, kvOpType.valueType),
      new MapFunction[KVOperation[K, V], Product]() {
        def map(in: KVOperation[K, V]) = (in.getRecord(), in.getValue())
      })
  }

  def toKVArrayStream(stream: DataStream[KVOperation[K, V]]): DataStream[_] = {
    val keyType = getKVOperationType().keyType
    val valueType = getKVOperationType().valueType
    val merger: SingleOutputStreamOperator[Array[Tuple2[K, V]], _] = stream.flatMap(
      new MultiGetMerger[K, V](keyType.createSerializer(getConfig()),
        valueType.createSerializer(getConfig()))).asInstanceOf[SingleOutputStreamOperator[Array[Tuple2[K, V]], _]]

    val tupleStream = merger.returns(ObjectArrayTypeInfo.getInfoFor(
      KVUtils.getKVType(keyType, valueType)).asInstanceOf[TypeInformation[Array[Tuple2[K, V]]]])
      .asInstanceOf[DataStream[Array[Tuple2[K, V]]]]

    KVUtils.nonCopyingMap(tupleStream,
      ObjectArrayTypeInfo.getInfoFor(createCaseClassTypeInfo(kvOpType.keyType, kvOpType.valueType)
        .asInstanceOf[TypeInformation[(K, V)]]),
      new MapFunction[Array[Tuple2[K, V]], Array[(K, V)]]() {
        def map(in: Array[Tuple2[K, V]]): Array[(K, V)] = {
          in.map(t => (t.f0, t.f1))
        }
      })
  }

  def toSKVArrayStream(stream: DataStream[KVOperation[K, V]], recordType: TypeInformation[_]): DataStream[_] = {
    val valueType = getKVOperationType().valueType
    val componentType = getComponentType(recordType).asInstanceOf[TypeInformation[Any]]

    val merger = stream.flatMap(new MultiGetMerger[K, V](componentType.createSerializer(getConfig()), valueType.createSerializer(getConfig())))
      .asInstanceOf[SingleOutputStreamOperator[Array[Tuple2[Any, V]], _]]

    val tupleStream = merger.returns(ObjectArrayTypeInfo.getInfoFor(
      KVUtils.getKVType(componentType, valueType)).asInstanceOf[TypeInformation[Array[Tuple2[Any, V]]]])
      .asInstanceOf[DataStream[Array[Tuple2[Any, V]]]]

    KVUtils.nonCopyingMap(tupleStream,
      ObjectArrayTypeInfo.getInfoFor(createCaseClassTypeInfo(componentType, valueType)
        .asInstanceOf[TypeInformation[(Any, V)]]),
      new MapFunction[Array[Tuple2[Any, V]], Array[(Any, V)]]() {
        def map(in: Array[Tuple2[Any, V]]): Array[(Any, V)] = {
          in.map(t => (t.f0, t.f1))
        }
      })
  }

  def getKVOperationType() = {
    if (kvOpType == null) {
      val tt: CaseClassTypeInfo[(K, V)] = kvType.asInstanceOf[CaseClassTypeInfo[(K, V)]]
      kvOpType = new KVOperationTypeInfo[K, V](tt.getTypeAt[K](0), tt.getTypeAt[V](1))
    }
    kvOpType
  }

  def createCaseClassTypeInfo(keyType: TypeInformation[_], valueType: TypeInformation[V]): CaseClassTypeInfo[Product] = {
    val keySerializer = new NullHandlerSerializer(keyType.createSerializer(getConfig()))
    val valueSerializer = new NullHandlerSerializer(valueType.createSerializer(getConfig()))

    val clazz = (keySerializer.createInstance(), valueSerializer.createInstance()).getClass().asInstanceOf[Class[Product]]

    new CaseClassTypeInfo[Product](clazz, Array(keyType, valueType), List(keyType, valueType), List("_1", "_2")) {
      def createSerializer(config: ExecutionConfig) =
        new CaseClassSerializer[Product](clazz, Array(keySerializer, valueSerializer)) {
          def createInstance(fields: Array[Object]) = (fields(0), fields(1))
        }
    }
  }

}