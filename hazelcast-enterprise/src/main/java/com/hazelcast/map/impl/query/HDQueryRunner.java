/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;

import java.util.Collection;

/**
 * Runs query operations in the calling thread (thus blocking it)
 * <p>
 * Used by query operations only: QueryOperation & QueryPartitionOperation
 * Should not be used by proxies or any other query related objects.
 */
public class HDQueryRunner extends QueryRunner {

    public HDQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer,
                         PartitionScanExecutor partitionScanExecutor, ResultProcessorRegistry resultProcessorRegistry) {
        super(mapServiceContext, optimizer, partitionScanExecutor, resultProcessorRegistry);
    }

    @Override
    protected Collection<QueryableEntry> runUsingPartitionScanSafely(String name, Predicate predicate,
                                                                     Collection<Integer> partitions, int migrationStamp) {
        // no partition-scan run in index+partition-scan query mode
        return null;
    }

}
