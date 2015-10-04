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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import streamkv.api.java.operator.checkpointing.KVMapCheckpointer;
import streamkv.api.java.types.KVOperation;
import streamkv.api.java.types.KVOperation.KVOperationType;
import streamkv.api.java.types.KVOperationSerializer;

import com.google.common.collect.Lists;

/**
 * Asynchronous implementation of the KVStore operator, which executes
 * operations in arrival order. The operator keeps the key-value pairs
 * partitioned among the operator instances, where each partition is kept in a
 * local {@link OperatorState} as a {@link HashMap}.
 * 
 * @param <K>
 *            Type of the keys.
 * @param <V>
 *            Type of the values.
 */
public class AsyncKVStoreOperator<K, V> extends AbstractUdfStreamOperator<KVOperation<K, V>, Function> implements
		OneInputStreamOperator<KVOperation<K, V>, KVOperation<K, V>> {

	private static final long serialVersionUID = 1L;

	private OperatorState<HashMap<K, V>> kvStore;
	protected KVOperationSerializer<K, V> kvOpSerializer;

	protected StreamRecord<KVOperation<K, V>> reuse;

	// Set of keys locked by the running transactions
	public Set<K> lockedKeys = new HashSet<>();
	// Operations waiting on one of the locks
	public Map<K, List<KVOperation<K, V>>> pendingOnLock = new HashMap<>();
	public Map<Long, Map<K, List<KVOperation<K, V>>>> rowsBeforeLock = new HashMap<>();
	// Running transaction ids and the counter for remaining operations per key
	public Map<Long, Map<K, Integer>> runningTransactions = new HashMap<>();
	// Operations part of some running transaction but waiting for inputs
	public Map<Long, Tuple2<KVOperation<K, V>, List<KVOperation<K, V>>>> runningOperations = new HashMap<>();
	// Operations inputs for other currently running ops
	public Map<Long, KVOperation<K, V>> inputs = new HashMap<>();
	// input id -> dependent op id
	public Map<Long, Long> inputMapping = new HashMap<>();

	@SuppressWarnings("serial")
	public AsyncKVStoreOperator(KVOperationSerializer<K, V> kvOpSerializer) {
		super(new Function() {
		});
		this.kvOpSerializer = kvOpSerializer;
	}

	@Override
	public void processElement(StreamRecord<KVOperation<K, V>> element) throws Exception {
		reuse = element;
		executeOperation(element.getValue());
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
	}

	@SuppressWarnings("unchecked")
	protected void executeOperation(KVOperation<K, V> op) throws Exception {
		K key = getKey(op);

		if (op.isPartOfTransaction) {
			// Operations part of a transaction
			if (runningTransactions.containsKey(op.transactionID)) {

				// Transaction has already started
				if (op.type.isResult) {
					// Input result for another operation
					if (inputMapping.containsKey(op.operationID)) {
						Tuple2<KVOperation<K, V>, List<KVOperation<K, V>>> runningOp = runningOperations
								.get(inputMapping.remove(op.operationID));
						runningOp.f1.add(op);
						if (runningOp.f1.size() == runningOp.f0.inputOperationIDs.length) {
							// already has all inputs
							executeWithoutInputs(internalizeInputs(runningOp.f0, runningOp.f1), getKey(runningOp.f0));
							runningOperations.remove(runningOp.f0.operationID);
						}
					} else {
						inputs.put(op.operationID, op);
					}
				} else {
					// Operation to execute
					executeTransactionalOp(op);
				}
			} else {

				Map<K, List<KVOperation<K, V>>> rows = rowsBeforeLock.get(op.transactionID);
				if (rows == null) {
					rows = new HashMap<>();
					rows.put(key, Lists.newArrayList(op));
					rowsBeforeLock.put(op.transactionID, rows);
				} else {
					List<KVOperation<K, V>> opList = rows.get(key);
					if (opList == null) {
						opList = new ArrayList<>();
						rows.put(key, opList);
					}
					opList.add(op);
				}
			}
		} else {
			// Operation not part of any running transaction
			if (lockedKeys.contains(key)) {
				// We received an operation for an already locked key
				addToPendingOnLock(key, op);
			} else {
				// Execute next operation
				if (op.type == KVOperationType.LOCK) {
					// Lock key and start transaction
					if (lockedKeys.contains(key)) {
						throw new RuntimeException("Sanity check: double lock.");
					}

					lockedKeys.add(key);

					Map<K, Integer> numOps = runningTransactions.get(op.operationID);

					if (numOps == null) {
						numOps = new HashMap<>();
						runningTransactions.put(op.operationID, numOps);
					}
					numOps.put(key, (int) op.numOpsForKey);

					Map<K, List<KVOperation<K, V>>> rows = rowsBeforeLock.get(op.operationID);
					if (rows != null) {
						List<KVOperation<K, V>> opList = rows.remove(key);
						if (opList != null) {
							for (KVOperation<K, V> o : opList) {
								executeOperation(o);
							}
						}
						if (rows.isEmpty()) {
							rowsBeforeLock.remove(op.operationID);
						}
					}
				} else {
					executeWithoutInputs(op, key);
				}
			}
		}
	}

	private K getKey(KVOperation<K, V> op) throws Exception {
		switch (op.type) {
		case PUT:
		case UPDATE:
		case GET:
		case MGET:
		case REMOVE:
		case LOCK:
		case KVRES:
		case MGETRES:
			return op.key;
		case SGET:
		case SGETRES:
		case SMGET:
		case SKVRES:
		case SUPDATE:
			return op.keySelector.getKey(op.record);
		default:
			throw new UnsupportedOperationException("Not implemented yet");
		}

	}

	private void executeTransactionalOp(KVOperation<K, V> op) throws IOException, Exception {
		if (op.inputOperationIDs.length > 0) {
			List<KVOperation<K, V>> inputOps = new ArrayList<>();
			for (long id : op.inputOperationIDs) {
				if (inputs.containsKey(id)) {
					inputOps.add(inputs.remove(id));
				} else {
					inputMapping.put(id, op.operationID);
				}
			}
			if (inputOps.size() == op.inputOperationIDs.length) {
				// already has all inputs
				executeWithoutInputs(internalizeInputs(op, inputOps), getKey(op));
			} else {
				if (runningOperations.containsKey(op.operationID)) {
					throw new RuntimeException("Sanity check: operation already running");
				}
				if (!runningTransactions.containsKey(op.transactionID)) {
					throw new RuntimeException("Sanity check: transaction not running");
				}
				runningOperations.put(op.operationID, Tuple2.of(op, inputOps));

			}
		} else {
			executeWithoutInputs(op, getKey(op));
		}
	}

	private KVOperation<K, V> internalizeInputs(KVOperation<K, V> op, List<KVOperation<K, V>> inputOps) {
		if ((op.type == KVOperationType.PUT || op.type == KVOperationType.UPDATE) && inputOps.size() == 1) {
			KVOperation<K, V> in = inputOps.get(0);
			if (in.type == KVOperationType.KVRES) {
				op.value = in.value;
				op.inputOperationIDs = new long[0];
				return op;
			}
		}
		throw new RuntimeException("Cannot internalize inputs");
	}

	private void addToPendingOnLock(K key, KVOperation<K, V> op) {
		List<KVOperation<K, V>> pending = pendingOnLock.get(key);
		if (pending == null) {
			pending = new LinkedList<>();
			pendingOnLock.put(key, pending);
		}
		pending.add(op);
	}

	private void executeWithoutInputs(KVOperation<K, V> op, K key) throws Exception {
		HashMap<K, V> store = kvStore.value();

		KVOperation<K, V> out = null;

		switch (op.type) {
		case PUT:
			store.put(key, op.value);
			break;
		case UPDATE:
			ReduceFunction<V> reduceFunction = op.reducer;
			if (!store.containsKey(key)) {
				store.put(key, op.value);
				out = KVOperation.kvRes(op.queryID, key, op.value);
			} else {
				// FIXME shall we copy here?
				V reduced = reduceFunction.reduce(store.get(key), op.value);
				store.put(key, reduced);
				out = KVOperation.kvRes(op.queryID, key, reduced);
			}
			break;
		case SUPDATE:
			ReduceFunction<V> rf = op.reducer;
			if (!store.containsKey(key)) {
				store.put(key, op.value);
				out = KVOperation.skvRes(op.queryID, op.record, op.value);
			} else {
				// FIXME shall we copy here?
				V reduced = rf.reduce(store.get(key), op.value);
				store.put(key, reduced);
				out = KVOperation.skvRes(op.queryID, op.record, reduced);
			}
			break;
		case GET:
			out = KVOperation.kvRes(op.queryID, key, store.get(key));
			break;
		case MGET:
			out = KVOperation.multiGetRes(op.queryID, key, store.get(key), op.numKeys, op.index, op.operationID);
			break;
		case REMOVE:
			out = KVOperation.kvRes(op.queryID, key, store.remove(key));
			break;
		case SGET:
			Object record = op.record;
			out = KVOperation.<K, V> skvRes(op.queryID, record, store.get(key));
			break;
		case SMGET:
			Object rec = op.record;
			out = KVOperation.<K, V> selectorMultiGetRes(op.queryID, rec, store.get(key), op.numKeys, op.index,
					op.operationID);
			break;
		default:
			throw new UnsupportedOperationException("Invalid operation: " + op);
		}
		kvStore.update(store);

		if (out != null) {
			// Set the output properties for transactional operations
			if (op.isPartOfTransaction) {
				out.isPartOfTransaction = true;
				out.dependentKey = op.dependentKey;
				out.transactionID = op.transactionID;
				out.inputOperationIDs = new long[0];
			}
			if (op.hasOpId) {
				out.hasOpId = true;
				out.operationID = op.operationID;
			}
			output.collect(reuse.replace(out));
		}

		if (op.isPartOfTransaction) {
			Map<K, Integer> runningTransaction = runningTransactions.get(op.transactionID);

			int count = runningTransaction.get(key) - 1;
			if (count == 0) {
				lockedKeys.remove(key);
				runningTransaction.remove(key);

				if (runningTransaction.isEmpty()) {
					runningTransactions.remove(op.transactionID);
				}

				List<KVOperation<K, V>> pendingOps = pendingOnLock.remove(key);
				if (pendingOps != null) {
					for (KVOperation<K, V> pending : pendingOps) {
						executeOperation(pending);
					}
				}
			} else if (count < 0) {
				throw new RuntimeException("Sanity check: at least 1 op");
			} else {
				runningTransaction.put(key, count);
			}

		}
	}

	@Override
	public void open(Configuration c) throws IOException {
		kvStore = getRuntimeContext().getOperatorState("kv-store", new HashMap<K, V>(), false,
				new KVMapCheckpointer<>(kvOpSerializer.keySerializer, kvOpSerializer.valueSerializer));
	}
}
