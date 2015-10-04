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

package streamkv.api.java.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import streamkv.api.java.types.KVOperation;
import streamkv.api.java.util.KVUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Operation {

	static Random rnd = new Random();

	private Operation() {

	}

	private static class Result<V> extends Operation {
	}

	public static class LiteralOp<V> extends Result<V> {
		V val;

		public LiteralOp(V val) {
			this.val = val;
		}
	}

	public static class PutOp<K, V> extends Operation {
		LiteralOp<K> K;
		Result<V> V;

		public PutOp(LiteralOp<K> K, Result<V> V) {
			this.K = K;
			this.V = V;
		}
	}

	public static class UpdateOp<K, V> extends Result<V> {
		LiteralOp<K> K;
		Result<V> V;

		public UpdateOp(LiteralOp<K> K, Result<V> V) {
			this.K = K;
			this.V = V;
		}
	}

	private static class AndOp extends Operation {

		public Operation[] ops;

		public AndOp(Operation... ops) {
			this.ops = ops;
		}
	}

	private static class GetOp<K, V> extends Result<V> {
		LiteralOp<K> K;

		public GetOp(LiteralOp<K> K) {
			this.K = K;
		}
	}

	private static class RemoveOp<K, V> extends Result<V> {
		public LiteralOp<K> K;

		public RemoveOp(LiteralOp<K> K) {
			this.K = K;
		}
	}

	public static <K, V> PutOp<K, V> Put(LiteralOp<K> key, Result<V> value) {
		return new PutOp<>(key, value);
	}

	public static <K, V> UpdateOp<K, V> Update(LiteralOp<K> key, Result<V> value) {
		return new UpdateOp<>(key, value);
	}

	public static <L> LiteralOp<L> Literal(L value) {
		return new LiteralOp<L>(value);
	}

	public static <K, V> GetOp<K, V> Get(LiteralOp<K> key) {
		return new GetOp<K, V>(key);
	}

	public static <K, V> RemoveOp<K, V> Remove(LiteralOp<K> key) {
		return new RemoveOp<K, V>(key);
	}

	public static Operation And(Operation... ops) {
		return new AndOp(ops);
	}

	public static List<KVOperation> createTransaction(int qid, Operation op) throws Exception {
		List<KVOperation> kvOps = new ArrayList<>();
		if (op instanceof LiteralOp) {
			throw new UnsupportedOperationException("Literals cannot produce any output");
		} else if (op instanceof AndOp) {
			for (Operation o : ((AndOp) op).ops) {
				kvOps.addAll(parseRecursively(o, new ArrayList<KVOperation>(), rnd.nextLong(), null));
			}
		} else {
			kvOps.addAll(parseRecursively(op, new ArrayList<KVOperation>(), rnd.nextLong(), null));
		}

		if (kvOps.size() > 1) {
			long transactionID = rnd.nextLong();
			Map<Object, Integer> keys = new HashMap<>();

			for (KVOperation kvOp : kvOps) {
				kvOp.setTransactionID(transactionID);
				kvOp.queryID = (short) qid;
				Object key = KVUtils.getK(kvOp);
				Integer c = keys.get(key);
				if (c == null) {
					keys.put(key, 1);
				} else {
					keys.put(key, c + 1);
				}
			}

			for (Map.Entry<Object, Integer> key : keys.entrySet()) {
				kvOps.add(0, KVOperation.lock(qid, key.getKey(), transactionID, key.getValue()));

			}
		}

		return kvOps;
	}

	private static List<KVOperation> parseRecursively(Operation op, List<KVOperation> l, long id, Object depKey) {
		if (op instanceof PutOp) {
			PutOp put = (PutOp) op;

			if (put.V instanceof LiteralOp) {
				l.add(KVOperation.put(0, put.K.val, ((LiteralOp) put.V).val).setDependentKey(depKey)
						.setInputIDs(new long[0]));
				return l;
			} else {
				long nextID = rnd.nextLong();
				l.add(KVOperation.put(0, put.K.val, null).setOpID(id).setInputIDs(new long[] { nextID })
						.setDependentKey(depKey));
				return parseRecursively(put.V, l, nextID, put.K.val);
			}

		} else if (op instanceof GetOp) {
			GetOp get = (GetOp) op;
			l.add(KVOperation.get(0, get.K.val).setOpID(id).setDependentKey(depKey).setInputIDs(new long[0]));
			return l;
		} else if (op instanceof GetOp) {
			RemoveOp rem = (RemoveOp) op;
			l.add(KVOperation.remove(0, rem.K.val).setOpID(id).setDependentKey(depKey).setInputIDs(new long[0]));
			return l;
		} else if (op instanceof UpdateOp) {
			UpdateOp put = (UpdateOp) op;

			if (put.V instanceof LiteralOp) {
				l.add(KVOperation.update(0, put.K.val, ((LiteralOp) put.V).val).setDependentKey(depKey)
						.setInputIDs(new long[0]));
				return l;
			} else {
				long nextID = rnd.nextLong();
				l.add(KVOperation.update(0, put.K.val, null).setOpID(id).setInputIDs(new long[] { nextID })
						.setDependentKey(depKey));
				return parseRecursively(put.V, l, nextID, put.K.val);
			}

		} else {
			throw new UnsupportedOperationException("??");
		}

	}
}
