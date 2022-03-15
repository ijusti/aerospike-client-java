/*
 * Copyright 2012-2022 Aerospike, Inc.
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
package com.aerospike.client.policy;

import com.aerospike.client.exp.Expression;

/**
 * Policy attributes used in batch UDF execute commands.
 */
public final class BatchUDFPolicy {
	/**
	 * Optional expression filter. If filterExp exists and evaluates to false, the specific batch key
	 * request is not performed and {@link com.aerospike.client.BatchRecord#resultCode} is set to
	 * {@link com.aerospike.client.ResultCode#FILTERED_OUT}.
	 * <p>
	 * If exists, this filter overrides the batch parent filter {@link com.aerospike.client.policy.Policy#filterExp}
	 * for the specific key in batch commands that allow a different policy per key.
	 * Otherwise, this filter is ignored.
	 * <p>
	 * Default: null
	 */
	public Expression filterExp;

	/**
	 * Desired consistency guarantee when committing a transaction on the server. The default
	 * (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	 * be successful before returning success to the client.
	 * <p>
	 * Default: CommitLevel.COMMIT_ALL
	 */
	public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

	/**
	 * Record expiration. Also known as ttl (time to live).
	 * Seconds record will live before being removed by the server.
	 * <p>
	 * Expiration values:
	 * <ul>
	 * <li>-2: Do not change ttl when record is updated.</li>
	 * <li>-1: Never expire.</li>
	 * <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
	 * <li>&gt; 0: Actual ttl in seconds.<br></li>
	 * </ul>
	 * <p>
	 * Default: 0
	 */
	public int expiration;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 * <p>
	 * Default: false (do not tombstone deleted records).
	 */
	public boolean durableDelete;

	/**
	 * Send user defined key in addition to hash digest.
	 * If true, the key will be stored with the record on the server.
	 * <p>
	 * Default: false (do not send the user defined key)
	 */
	public boolean sendKey;

	/**
	 * Copy constructor.
	 */
	public BatchUDFPolicy(BatchUDFPolicy other) {
		this.filterExp = other.filterExp;
		this.commitLevel = other.commitLevel;
		this.expiration = other.expiration;
		this.durableDelete = other.durableDelete;
		this.sendKey = other.sendKey;
	}

	/**
	 * Default constructor.
	 */
	public BatchUDFPolicy() {
	}
}
