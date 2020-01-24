/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.query.Statement;

public final class AsyncQueryPartition extends AsyncMultiCommand {
	private final RecordSequenceListener listener;
	private final Statement statement;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public AsyncQueryPartition(
		AsyncMultiExecutor parent,
		QueryPolicy policy,
		RecordSequenceListener listener,
		Statement statement,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(parent, nodePartitions.node, policy, tracker.socketTimeout, tracker.totalTimeout);
		this.listener = listener;
		this.statement = statement;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
	}

	@Override
	protected void writeBuffer() {
		setQuery(policy, statement, false, nodePartitions);
	}

	@Override
	protected void parseRow(Key key) {
		if ((info3 & Command.INFO3_PARTITION_DONE) != 0) {
			tracker.partitionDone(nodePartitions, generation);
			return;
		}
		tracker.setDigest(key);

		Record record = parseRecord();
		listener.onRecord(key, record);
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		if (tracker.shouldRetry(ae)) {
			parent.childSuccess(node);
			return;
		}
		parent.childFailure(ae);
	}
}