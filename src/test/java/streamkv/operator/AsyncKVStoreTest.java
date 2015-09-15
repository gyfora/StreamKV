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

package streamkv.operator;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import streamkv.types.KVOperation;

public class AsyncKVStoreTest {

	@Test
	public void testKVOperator() throws Exception {

		AsyncKVStoreOperator<String, Integer> operator = new AsyncKVStoreOperator<>();

		OneInputStreamOperatorTestHarness<KVOperation<String, Integer>, KVOperation<String, Integer>> testHarness = new OneInputStreamOperatorTestHarness<>(
				operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1)));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "b", 2)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));

		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(1, "a", 1)));

		TestHarnessUtil
				.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}
}
