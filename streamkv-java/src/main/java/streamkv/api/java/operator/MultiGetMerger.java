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

import java.io.IOException;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import streamkv.api.java.operator.checkpointing.MergeStateCheckpointer;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperation.KVOperationType;

/**
 * {@link FlatMapFunction} to merge the different key-value pairs for multiget
 * operations.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
@SuppressWarnings("rawtypes")
public class MultiGetMerger<K, V> extends RichFlatMapFunction<KVOperation<K, V>, Tuple2[]> {
	private static final long serialVersionUID = 1L;

	private OperatorState<Tuple2<Integer, Tuple2[]>> merged;
	private TypeSerializer keySerializer;
	private TypeSerializer valueSerializer;

	public MultiGetMerger(TypeSerializer keySerializer, TypeSerializer valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public void flatMap(KVOperation<K, V> next, Collector<Tuple2[]> out) throws Exception {
		Tuple2<Integer, Tuple2[]> partial = merged.value();
		Object key = next.type == KVOperationType.MGETRES ? next.key : next.record;
		short numKeys = next.numKeys;

		if (numKeys == 0) {
			throw new RuntimeException("Number of keys must be at least 1");
		}

		if (partial.f0 == -1) {
			partial.f0 = (int) numKeys - 1;
			partial.f1 = new Tuple2[numKeys];
			partial.f1[0] = Tuple2.of(key, next.value);

		} else {
			partial.f0 -= 1;
			partial.f1[numKeys - partial.f0 - 1] = Tuple2.of(key, next.value);
		}

		if (partial.f0 == 0) {
			out.collect(partial.f1);
			merged.update(null);
		} else {
			merged.update(partial);
		}

	}

	@Override
	public void open(Configuration conf) throws IOException {
		merged = getRuntimeContext().getOperatorState("merged",
				Tuple2.<Integer, Tuple2[]> of(-1, new Tuple2[0]), true,
				new MergeStateCheckpointer<>(keySerializer, valueSerializer));
	}
}
