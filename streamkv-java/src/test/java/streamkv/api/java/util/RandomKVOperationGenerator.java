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

package streamkv.api.java.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperation.KVOperationType;
import streamkv.api.java.types.KVOperationSerializer;

import com.google.common.collect.ImmutableMap;

public class RandomKVOperationGenerator {

	private Random rnd = new Random();
	@SuppressWarnings("rawtypes")
	public static KVOperationSerializer<Integer, Integer> opSerializer = new KVOperationSerializer<>(
			IntSerializer.INSTANCE, IntSerializer.INSTANCE, new HashMap<Short, ReduceFunction<Integer>>(),
			ImmutableMap.of((short) 0, Tuple2.<TypeSerializer, KeySelector> of(new StringSerializer(), null)), null);

	public KVOperation<Integer, Integer>[] generate(int numOperations) {

		@SuppressWarnings("unchecked")
		KVOperation<Integer, Integer>[] output = new KVOperation[numOperations];

		for (int i = 0; i < numOperations; i++) {
			output[i] = generateOp();
		}

		return output;
	}

	public List<KVOperation<Integer, Integer>> generateList(int numOperations) {

		List<KVOperation<Integer, Integer>> ops = new LinkedList<>();

		for (int i = 0; i < numOperations; i++) {
			ops.add(generateOp());
		}

		return ops;
	}

	public KVOperationSerializer<Integer, Integer> getSerializer() {
		return opSerializer;
	}

	public KVOperation<Integer, Integer> generateOp() {
		KVOperationType type = KVOperation.types[rnd.nextInt(KVOperation.types.length)];
		KVOperation<Integer, Integer> op;
		switch (type) {
		case GET:
			op = KVOperation.<Integer, Integer> get(rnd.nextInt(), rnd.nextInt());
			break;
		case KVRES:
			op = KVOperation.<Integer, Integer> kvRes(rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
			break;
		case MGET:
			op = KVOperation.<Integer, Integer> multiGet(rnd.nextInt(), rnd.nextInt(), (short) rnd.nextInt(),
					(short) rnd.nextInt(), rnd.nextLong());
			break;
		case MGETRES:
			op = KVOperation.<Integer, Integer> multiGetRes(rnd.nextInt(), rnd.nextInt(), rnd.nextInt(),
					(short) rnd.nextInt(), (short) rnd.nextInt(), rnd.nextLong());
			break;
		case PUT:
			op = KVOperation.<Integer, Integer> put(rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
			break;
		case REMOVE:
			op = KVOperation.<Integer, Integer> remove(rnd.nextInt(), rnd.nextInt());
			break;
		case SGET:
			op = KVOperation.<Integer, Integer> selectorGet(0, ((Integer) rnd.nextInt()).toString());
			break;
		case SGETRES:
			op = KVOperation.<Integer, Integer> skvRes(0, ((Integer) rnd.nextInt()).toString(), rnd.nextInt());
			break;
		case SMGET:
			op = KVOperation.<Integer, Integer> selectorMultiGet(0, ((Integer) rnd.nextInt()).toString(),
					(short) rnd.nextInt(), (short) rnd.nextInt(), rnd.nextLong());
			break;
		case SKVRES:
			op = KVOperation.<Integer, Integer> selectorMultiGetRes(0, ((Integer) rnd.nextInt()).toString(),
					rnd.nextInt(), (short) rnd.nextInt(), (short) rnd.nextInt(), rnd.nextLong());
			break;
		case UPDATE:
			op = KVOperation.update(0, rnd.nextInt(), rnd.nextInt());
			break;
		case LOCK:
			op = KVOperation.lock(0, rnd.nextInt(), rnd.nextLong(), rnd.nextInt());
			break;
		case SUPDATE:
			op = KVOperation.selectorUpdate(0, rnd.nextInt(), ((Integer) rnd.nextInt()).toString());
			break;
		default:
			throw new RuntimeException("Error in generator");
		}

		if (rnd.nextBoolean()) {
			op.setOpID(rnd.nextLong());
		}
		if (rnd.nextBoolean()) {
			op.setTransactionID(rnd.nextLong());
			if (rnd.nextBoolean()) {
				op.setDependentKey(rnd.nextInt());
			}
			if (rnd.nextBoolean()) {
				op.setInputIDs(new long[] { rnd.nextLong(), rnd.nextLong() });
			} else {
				op.setInputIDs(new long[0]);
			}
		}

		return op;
	}
}
