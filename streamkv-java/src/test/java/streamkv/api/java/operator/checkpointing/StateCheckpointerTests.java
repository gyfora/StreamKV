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

package streamkv.api.java.operator.checkpointing;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;

import scala.util.Random;
import streamkv.api.java.operator.checkpointing.KVMapCheckpointer;
import streamkv.api.java.operator.checkpointing.MergeStateCheckpointer;
import streamkv.api.java.operator.checkpointing.PendingOperationCheckpointer;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.util.RandomKVOperationGenerator;

public class StateCheckpointerTests {

	@Test
	public void testKVMapCheckpointer() throws IOException, ClassNotFoundException {

		StateCheckpointer<HashMap<Integer, String>, byte[]> cp = new KVMapCheckpointer<Integer, String>(
				IntSerializer.INSTANCE, StringSerializer.INSTANCE);

		HashMap<Integer, String> state = new HashMap<>();
		state.put(1, "a");
		state.put(2, "b");
		state.put(3, "c");
		state.put(4, "d");

		byte[] serializableState = cp.snapshotState(state, 0, 0);
		byte[] serialized = InstantiationUtil.serializeObject(serializableState);

		assertEquals(state, cp.restoreState((byte[]) InstantiationUtil.deserializeObject(serialized, Thread
				.currentThread().getContextClassLoader())));

	}

	@Test
	public void testPendingOpCheckpointer() throws IOException, ClassNotFoundException {

		Random rnd = new Random();
		RandomKVOperationGenerator opGen = new RandomKVOperationGenerator();

		StateCheckpointer<TreeMap<Long, List<KVOperation<Integer, Integer>>>, byte[]> cp = new PendingOperationCheckpointer<>(
				RandomKVOperationGenerator.opSerializer);

		TreeMap<Long, List<KVOperation<Integer, Integer>>> pending = new TreeMap<>();
		for (int i = 0; i < 1000; i++) {
			LinkedList<KVOperation<Integer, Integer>> ops = new LinkedList<>(opGen.generateList(rnd
					.nextInt(10)));
			pending.put(rnd.nextLong(), ops);
		}

		byte[] serializableState = cp.snapshotState(pending, 0, 0);
		byte[] serialized = InstantiationUtil.serializeObject(serializableState);

		assertEquals(pending, cp.restoreState((byte[]) InstantiationUtil.deserializeObject(serialized, Thread
				.currentThread().getContextClassLoader())));

	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testMergeStateCheckpointer() throws IOException, ClassNotFoundException {

		StateCheckpointer<Tuple2<Integer, Tuple2[]>, byte[]> cp = new MergeStateCheckpointer<>(
				IntSerializer.INSTANCE, StringSerializer.INSTANCE);

		Tuple2<Integer, Tuple2[]> state = Tuple2.of(5, new Tuple2[] { Tuple2.of(2, "a"), Tuple2.of(4, "b"),
				Tuple2.of(5, (String) null), null });

		byte[] serializableState = cp.snapshotState(state, 0, 0);
		byte[] serialized = InstantiationUtil.serializeObject(serializableState);

		Tuple2<Integer, Tuple2[]> restored = cp.restoreState((byte[]) InstantiationUtil.deserializeObject(
				serialized, Thread.currentThread().getContextClassLoader()));
		assertEquals(state.f0, restored.f0);
		assertArrayEquals(state.f1, restored.f1);
	}
}
