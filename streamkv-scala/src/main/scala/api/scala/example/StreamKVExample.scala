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
import java.util.Arrays

object StreamKVExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)

    // Create a store for account information (name, balance)
    val store = KVStore[String, Double](ARRIVALTIME)

    // Expected input:
    // "PUT name, balance"
    // "GET name"
    // "MGET name,name,..."
    // "TRANSFER from, to, amount"
    val inputStream: DataStream[(String, String)] = env.socketTextStream("localhost", 9999).flatMap((in, c) => {
      if (in == "fail") { throw new RuntimeException("Failed") }
      val split = in.split(" ")
      try {
        c.collect((split(0).toUpperCase(), split.slice(1, split.length).fold("")(_ + _)))
      } catch {
        case e: Exception => System.err.println("Parsing error: " + in)
      }
    })

    // Read the initial balance as a stream
    val initialBalance = inputStream.filter(_._1 == "PUT").map(x => {
      val split = x._2.split(",")
      (split(0), split(1).toDouble)
    })

    // Feed the balance stream into the store
    store.put(initialBalance)

    // At any time query the balance by name
    val balanceQ = store.get(inputStream.filter(_._1 == "GET").map(_._2))

    // At any time query the balance for multiple people
    val totalBalanceQ = store.multiGet(inputStream.filter(_._1 == "MGET").map(_._2.split(",")))

    // Transfer : (from, to, amount)
    val transferStream: DataStream[(String, String, Double)] = inputStream.filter(_._1 == "TRANSFER").map(x => {
      val split = x._2.split(",")
      (split(0), split(1), split(2).toDouble)
    })

    // Apply transfer by subtracting from the sender and adding to the receiver
    store.update(transferStream.flatMap(x => Array((x._1, -1 * x._3), (x._2, x._3))))((b1, b2) => b1 + b2)

    // Print the query outputs
    balanceQ.getOutput.print
    totalBalanceQ.getOutput.addSink(x => println(x.mkString(",")))

    env.execute
  }

}