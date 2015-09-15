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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import streamkv.types.KVOperation;

public class AsyncKVStoreOperatorTest {

	@Test
	public void testKVOperator() throws Exception {

		AsyncKVStoreOperator<String, Integer> operator = new AsyncKVStoreOperator<>();

		OneInputStreamOperatorTestHarness<KVOperation<String, Integer>, KVOperation<String, Integer>> testHarness = new OneInputStreamOperatorTestHarness<>(
				operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1)));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "1", 2)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "1")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c")));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 4)));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "c", 3)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "1")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> remove(3, "c")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c")));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> remove(3, "d")));
		testHarness.processElement(new StreamRecord<>(selectorGet(4, 1, selector)));
		testHarness.processElement(new StreamRecord<>(selectorGet(4, 2, selector)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> multiGet(5, "a",
				(short) 5, 1L)));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> multiGet(5, "d",
				(short) 5, 2L)));

		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(1, "a", 1)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(1, "1", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(2, "c", null)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(1, "a", 4)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(2, "1", 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(2, "c", 3)));
		expectedOutput.add(new StreamRecord<>(KVOperation.removeRes(3, "c", 3)));
		expectedOutput.add(new StreamRecord<>(KVOperation.getRes(2, "c", null)));
		expectedOutput.add(new StreamRecord<>(KVOperation.removeRes(3, "d", null)));
		expectedOutput.add(new StreamRecord<>(KVOperation.selectorGetRes(4, 1, 2)));
		expectedOutput.add(new StreamRecord<>(KVOperation.selectorGetRes(4, 2, null)));
		expectedOutput.add(new StreamRecord<>(KVOperation.multiGetRes(5, "a", 4, (short) 5, 1L)));
		expectedOutput.add(new StreamRecord<>(KVOperation.multiGetRes(5, "d", null, (short) 5, 2L)));

		TestHarnessUtil
				.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	public static KeySelector<Object, String> selector = new KeySelector<Object, String>() {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Object value) throws Exception {
			return value.toString();
		}
	};

	public static <X> KVOperation<String, Integer> selectorGet(int id, X record,
			KeySelector<Object, String> selector) {
		KVOperation<String, Integer> op = KVOperation.<String, Integer> selectorGet(id, record);
		op.setKeySelector(selector);
		return op;
	}
}
