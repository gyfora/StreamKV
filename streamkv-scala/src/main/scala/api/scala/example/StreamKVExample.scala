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

package api.scala.example

import org.apache.flink.streaming.api.scala._

import api.scala.KVStore
import api.scala.Query
import streamkv.api.java.OperationOrdering.ARRIVALTIME

object StreamKVExample {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val store = KVStore[Int, Person](ARRIVALTIME)
    
    val persons: DataStream[Person] = env.socketTextStream("localhost", 9999).flatMap((in, c) => {
      val split = in.split(",")
      try {
        c.collect(Person(split(0).toInt, split(1), split(2).toInt))
      } catch {
        case e: Exception => System.err.println("Parsing error: " + in);
      }
    })

    store.put(persons.map(p => (p.id, p)))

    val personQuery = store.get(env.socketTextStream("localhost", 9998).map(_.toInt))
    val personArrayQuery = store.multiGet(env.socketTextStream("localhost", 9997).map(_.split(",").map(_.toInt)))

    personQuery.getOutput.print
    personArrayQuery.getOutput.filter(_.length == 2).map(a => ((a(0)._1, a(1)._1), Math.abs(a(0)._2.age - a(1)._2.age))).print
    env.execute

  }

}