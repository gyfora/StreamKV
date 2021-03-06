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

package streamkv.api.java.operator;

import static streamkv.api.java.operator.AsyncKVStoreOperatorTest.sum;
import static streamkv.api.java.operator.AsyncKVStoreOperatorTest.selector;
import static streamkv.api.java.operator.AsyncKVStoreOperatorTest.selectorGet;
import static streamkv.api.java.operator.AsyncKVStoreOperatorTest.selectorMultiGet;
import static streamkv.api.java.operator.AsyncKVStoreOperatorTest.update;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationSerializer;

public class TimestampedKVStoreOperatorTest {

	@Test
	public void testKVOperator() throws Exception {

		TimestampedKVStoreOperator<String, Integer> operator = new TimestampedKVStoreOperator<>(
				new KVOperationSerializer<String, Integer>(StringSerializer.INSTANCE, IntSerializer.INSTANCE, null,
						null, null));

		OneInputStreamOperatorTestHarness<KVOperation<String, Integer>, KVOperation<String, Integer>> testHarness = new OneInputStreamOperatorTestHarness<>(
				operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 1), 1));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "a", 4), 6));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> remove(3, "c"), 11));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "1", 2), 2));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "1"), 4));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> multiGet(5, "d", (short) 5,
				(short) 1, 2L), 17));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c"), 5));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a"), 3));

		testHarness.processWatermark(new Watermark(6));

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> remove(3, "d"), 13));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "1"), 9));
		testHarness.processElement(new StreamRecord<>(KVOperation.put(0, "c", 3), 7));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c"), 10));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(1, "a"), 8));

		testHarness.processWatermark(new Watermark(11));

		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> multiGet(5, "a", (short) 5,
				(short) 0, 1L), 16));
		testHarness.processElement(new StreamRecord<>(selectorGet(4, 1, selector), 14));
		testHarness.processElement(new StreamRecord<>(selectorGet(4, 2, selector), 15));
		testHarness.processElement(new StreamRecord<>(selectorMultiGet(6, 1, 5, 0, 2L, selector), 17));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(2, "c"), 12));
		testHarness.processElement(new StreamRecord<>(update(7, "z", 10, sum), 21));

		testHarness.processWatermark(new Watermark(18));

		testHarness.processElement(new StreamRecord<>(update(7, "z", 1, sum), 19));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(8, "z"), 20));
		testHarness.processElement(new StreamRecord<>(KVOperation.<String, Integer> get(8, "z"), 22));

		testHarness.processWatermark(new Watermark(22));

		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "a", 1), 3));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "1", 2), 4));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "c", null), 5));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(1, "a", 4), 8));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "1", 2), 9));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "c", 3), 10));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(3, "c", 3), 11));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(2, "c", null), 12));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(3, "d", null), 13));
		expectedOutput.add(new StreamRecord<>(KVOperation.skvRes(4, 1, 2), 14));
		expectedOutput.add(new StreamRecord<>(KVOperation.skvRes(4, 2, null), 15));
		expectedOutput.add(new StreamRecord<>(KVOperation.multiGetRes(5, "a", 4, (short) 5, (short) 0, 1L), 16));
		expectedOutput.add(new StreamRecord<>(KVOperation.multiGetRes(5, "d", null, (short) 5, (short) 1, 2L), 17));
		expectedOutput.add(new StreamRecord<>(KVOperation.selectorMultiGetRes(6, 1, 2, (short) 5, (short) 0, 2L), 17));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(7, "z", 1), 19));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(8, "z", 1), 20));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(7, "z", 11), 21));
		expectedOutput.add(new StreamRecord<>(KVOperation.kvRes(8, "z", 11), 22));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}
}
