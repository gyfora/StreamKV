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

package streamkv.api.scala.example

import org.apache.flink.streaming.api.scala._
import streamkv.api.scala.KVStore
import streamkv.api.java.OperationOrdering.ARRIVALTIME

object StreamKVExample {

  /**
   * This example shows an example application using the key value store with
   * operations from text sockets. To run the example make sure that the service
   * providing the text data is already up and running.
   * <p>
   * To start an example socket text stream on your local machine run netcat from
   * a command line: <code>nc -lk 9999</code>
   * <p>
   * Valid inputs:
   * <ul>
   * <li>"put name, amount"</li>
   * <li>"get name"</li>
   * <li>"mget name1,name2,..."</li>
   * <li>"transfer from, to, amount"</li>
   * </ul>
   *
   * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
   */
  def main(args: Array[String]): Unit = {

    // Get the environment and enable checkpointing
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)

    // Create a store for account information (name, balance)
    val store = KVStore[String, Double](ARRIVALTIME)

    // Read and parse the input stream from the text socket
    val inputStream: DataStream[(String, String)] = env.socketTextStream("localhost", 9999).flatMap((in, c) => {
      if (in == "fail") { throw new RuntimeException("Failed") }
      val split = in.split(" ")
      try {
        c.collect((split(0).toUpperCase(), split.slice(1, split.length).fold("")(_ + _)))
      } catch {
        case e: Exception => System.err.println("Parsing error: " + in)
      }
    })

    // Convert the put stream to Tuple2-s
    val initialBalance = inputStream.filter(_._1 == "PUT").map(x => {
      val split = x._2.split(",")
      (split(0), split(1).toDouble)
    })

    // Feed the balance stream into the store
    store.put(initialBalance)

    // At any time query the balance by name
    val balanceQ = store.get(inputStream.filter(_._1 == "GET").map(_._2))

    // At any time query the balance for multiple people
    val mBalanceQ = store.multiGet(inputStream.filter(_._1 == "MGET").map(_._2.split(",")))

    // Parse the transfer stream to (from, to, amount)
    val transferStream: DataStream[(String, String, Double)] = inputStream.filter(_._1 == "TRANSFER")
      .map(x => {
        val split = x._2.split(",")
        (split(0), split(1), split(2).toDouble)
      })

    // Apply transfer by subtracting from the sender and adding to the receiver
    store.update(transferStream.flatMap(x => List((x._1, -1 * x._3), (x._2, x._3))))(_ + _)

    // Print the query outputs
    balanceQ.getOutput.print
    mBalanceQ.getOutput.addSink(x => println(x.mkString(",")))

    // Execute the program
    env.execute
  }

}
