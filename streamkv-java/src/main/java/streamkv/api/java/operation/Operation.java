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

import org.apache.flink.api.java.tuple.Tuple2;

import streamkv.api.java.KVStore;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.util.KVUtils;

@SuppressWarnings("unchecked")
public class Operation<K, V> {
	static Random rnd = new Random();

	private static class Result<K, V> extends Operation<K, V> {
	}

	public static class LiteralOp<V> extends Result<Void, V> {
		V val;

		public LiteralOp(V val) {
			this.val = val;
		}
	}

	public static class PutOp<K, V> extends Operation<K, V> {
		LiteralOp<K> K;
		Result<K, V> V;

		public PutOp(LiteralOp<K> K, Result<K, V> V) {
			this.K = K;
			this.V = V;
		}
	}

	public static class UpdateOp<K, V> extends Result<K, V> {
		LiteralOp<K> K;
		Result<K, V> V;

		public UpdateOp(LiteralOp<K> K, Result<K, V> V) {
			this.K = K;
			this.V = V;
		}
	}

	private static class AndOp<K, V> extends Operation<K, V> {

		public Operation<K, V>[] ops;

		public AndOp(Operation<K, V>... ops) {
			this.ops = ops;
		}
	}

	private static class GetOp<K, V> extends Result<K, V> {
		LiteralOp<K> K;

		public GetOp(LiteralOp<K> K) {
			this.K = K;
		}
	}

	private static class RemoveOp<K, V> extends Result<K, V> {
		public LiteralOp<K> K;

		public RemoveOp(LiteralOp<K> K) {
			this.K = K;
		}
	}

	public static <K, V> PutOp<K, V> Put(LiteralOp<K> key, Result<K, V> value) {
		return new PutOp<>(key, value);
	}

	public static <K, V> UpdateOp<K, V> Update(LiteralOp<K> key, Result<K, V> value) {
		return new UpdateOp<>(key, value);
	}

	public static <K> LiteralOp<K> Literal(K value) {
		return new LiteralOp<K>(value);
	}

	public static <K, V> GetOp<K, V> Get(LiteralOp<K> key, KVStore<K, V> store) {
		return new GetOp<K, V>(key);
	}

	public static <K, V> RemoveOp<K, V> Remove(LiteralOp<K> key, KVStore<K, V> store) {
		return new RemoveOp<K, V>(key);
	}

	@SafeVarargs
	public static <K, V> Operation<K, V> And(Operation<K, V>... ops) {
		return new AndOp<K, V>(ops);
	}

	public static <K, V> List<KVOperation<K, V>> createTransaction(int qid, Operation<K, V> op) throws Exception {
		List<KVOperation<K, V>> kvOps = new ArrayList<>();
		if (op instanceof LiteralOp) {
			throw new UnsupportedOperationException("Literals cannot be standalone operations.");
		} else if (op instanceof AndOp) {
			for (Operation<K, V> o : ((AndOp<K, V>) op).ops) {
				kvOps.addAll(parseRecursively(o, new ArrayList<KVOperation<K, V>>(), rnd.nextLong(), null));
			}
		} else {
			kvOps.addAll(parseRecursively(op, new ArrayList<KVOperation<K, V>>(), rnd.nextLong(), null));
		}

		if (kvOps.size() > 1) {
			long transactionID = rnd.nextLong();
			Map<K, Tuple2<Integer, Boolean>> keys = new HashMap<>();

			for (KVOperation<K, V> kvOp : kvOps) {
				kvOp.setTransactionID(transactionID);
				kvOp.queryID = (short) qid;
				K key = KVUtils.getK(kvOp);
				Tuple2<Integer, Boolean> t = keys.get(key);
				if (t == null) {
					keys.put(key, Tuple2.of(1, kvOp.inputOperationIDs.length > 0));
				} else {
					t.f0 = t.f0 + 1;
					t.f1 = t.f1 || kvOp.inputOperationIDs.length > 0;
					keys.put(key, t);
				}
			}

			for (Map.Entry<K, Tuple2<Integer, Boolean>> key : keys.entrySet()) {
				if (key.getValue().f0 > 1 || key.getValue().f1) {
					kvOps.add(0, KVOperation.<K, V> lock(qid, key.getKey(), transactionID, key.getValue().f0));
				}
			}
		}

		return kvOps;
	}

	private static <K, V> List<KVOperation<K, V>> parseRecursively(Operation<K, V> op, List<KVOperation<K, V>> l,
			long id, K depKey) {
		if (op instanceof PutOp) {
			PutOp<K, V> put = (PutOp<K, V>) op;

			if (put.V instanceof LiteralOp) {
				l.add(KVOperation.put(0, put.K.val, ((LiteralOp<V>) put.V).val).setDependentKey(depKey)
						.setInputIDs(new long[0]));
				return l;
			} else {
				long nextID = rnd.nextLong();
				l.add(KVOperation.<K, V> put(0, put.K.val, null).setOpID(id).setInputIDs(new long[] { nextID })
						.setDependentKey(depKey));
				return parseRecursively(put.V, l, nextID, put.K.val);
			}

		} else if (op instanceof GetOp) {
			GetOp<K, V> get = (GetOp<K, V>) op;
			l.add(KVOperation.<K, V> get(0, get.K.val).setOpID(id).setDependentKey(depKey).setInputIDs(new long[0]));
			return l;
		} else if (op instanceof GetOp) {
			RemoveOp<K, V> rem = (RemoveOp<K, V>) op;
			l.add(KVOperation.<K, V> remove(0, rem.K.val).setOpID(id).setDependentKey(depKey).setInputIDs(new long[0]));
			return l;
		} else if (op instanceof UpdateOp) {
			UpdateOp<K, V> put = (UpdateOp<K, V>) op;

			if (put.V instanceof LiteralOp) {
				l.add(KVOperation.update(0, put.K.val, ((LiteralOp<V>) put.V).val).setDependentKey(depKey)
						.setInputIDs(new long[0]));
				return l;
			} else {
				long nextID = rnd.nextLong();
				l.add(KVOperation.<K, V> update(0, put.K.val, null).setOpID(id).setInputIDs(new long[] { nextID })
						.setDependentKey(depKey));
				return parseRecursively(put.V, l, nextID, put.K.val);
			}

		} else {
			throw new UnsupportedOperationException("Unknown operation.");
		}

	}
}
