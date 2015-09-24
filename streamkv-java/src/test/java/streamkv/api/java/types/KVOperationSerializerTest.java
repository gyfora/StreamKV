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
package streamkv.api.java.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestInputView;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestOutputView;
import org.junit.Test;

import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperationTypeInfo.KVOpSerializer;
import streamkv.api.java.util.RandomKVOperationGenerator;

public class KVOperationSerializerTest {

	private RandomKVOperationGenerator gen = new RandomKVOperationGenerator();

	@Test
	public void testCopy() {

		Random rnd = new Random();

		KVOperation<Integer, Integer>[] ops = gen.generate(1000);
		KVOpSerializer<Integer, Integer> serializer = gen.getSerializer();

		for (KVOperation<Integer, Integer> op : ops) {
			assertEquals(op, serializer.copy(op));
		}

		for (KVOperation<Integer, Integer> op : ops) {
			assertEquals(op, serializer.copy(op, ops[rnd.nextInt(ops.length)]));
		}
	}

	@Test
	public void testSerialization() throws IOException {
		Random rnd = new Random();

		KVOperation<Integer, Integer>[] ops = gen.generate(1000);
		KVOpSerializer<Integer, Integer> serializer = gen.getSerializer();

		for (KVOperation<Integer, Integer> op : ops) {
			TestOutputView out = new TestOutputView();
			serializer.serialize(op, out);
			TestInputView in = out.getInputView();
			assertEquals(op, serializer.deserialize(in));
		}

		for (KVOperation<Integer, Integer> op : ops) {
			TestOutputView out = new TestOutputView();
			serializer.serialize(op, out);
			TestInputView in = out.getInputView();
			assertEquals(op, serializer.deserialize(ops[rnd.nextInt(ops.length)], in));
		}
	}
}
