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

package streamkv.util;

import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import streamkv.types.KVOperation;
import streamkv.types.KVOperation.KVOperationType;
import streamkv.types.KVOperationTypeInfo.KVOpSerializer;

import com.google.common.collect.ImmutableMap;

public class RandomKVOperationGenerator {

	private Random rnd = new Random();
	@SuppressWarnings("rawtypes")
	private KVOpSerializer<Integer, Integer> serializer = new KVOpSerializer<>(new IntSerializer(),
			new IntSerializer(), ImmutableMap.of(0,
					Tuple2.<TypeSerializer, KeySelector> of(new StringSerializer(), null)), null);

	public KVOperation<Integer, Integer>[] generate(int numOperations) {

		@SuppressWarnings("unchecked")
		KVOperation<Integer, Integer>[] output = new KVOperation[numOperations];

		for (int i = 0; i < numOperations; i++) {
			output[i] = generateOp();
		}

		return output;
	}

	public KVOpSerializer<Integer, Integer> getSerializer() {
		return serializer;
	}

	public KVOperation<Integer, Integer> generateOp() {
		KVOperationType type = KVOperation.types[rnd.nextInt(KVOperation.types.length)];

		switch (type) {
		case GET:
			return KVOperation.<Integer, Integer> get(rnd.nextInt(), rnd.nextInt());
		case GETRES:
			return KVOperation.<Integer, Integer> getRes(rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
		case MGET:
			return KVOperation.<Integer, Integer> multiGet(rnd.nextInt(), rnd.nextInt(),
					(short) rnd.nextInt(), rnd.nextLong());
		case MGETRES:
			return KVOperation.<Integer, Integer> multiGetRes(rnd.nextInt(), rnd.nextInt(), rnd.nextInt(),
					(short) rnd.nextInt(), rnd.nextLong());
		case PUT:
			return KVOperation.<Integer, Integer> put(rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
		case REMOVE:
			return KVOperation.<Integer, Integer> remove(rnd.nextInt(), rnd.nextInt());
		case REMOVERES:
			return KVOperation.<Integer, Integer> removeRes(rnd.nextInt(), rnd.nextInt(), rnd.nextInt());
		case SGET:
			return KVOperation.<Integer, Integer> selectorGet(0, ((Integer) rnd.nextInt()).toString());
		case SGETRES:
			return KVOperation.<Integer, Integer> selectorGetRes(0, ((Integer) rnd.nextInt()).toString(),
					rnd.nextInt());
		case SMGET:
			return KVOperation.<Integer, Integer> selectorMultiGet(0, ((Integer) rnd.nextInt()).toString(),
					(short) rnd.nextInt(), rnd.nextLong());
		case SMGETRES:
			return KVOperation.<Integer, Integer> selectorMultiGetRes(0,
					((Integer) rnd.nextInt()).toString(), rnd.nextInt(), (short) rnd.nextInt(),
					rnd.nextLong());
		default:
			break;
		}
		throw new RuntimeException("Error in generator");
	}
}
