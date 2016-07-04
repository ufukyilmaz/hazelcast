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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.spi.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Map;

/**
 * Inserts the {@link MapEntries} for all partitions of a member via locally invoked {@link PutAllOperation}.
 * <p/>
 * Used to reduce the number of remote invocations of an {@link com.hazelcast.core.IMap#putAll(Map)} call.
 */
public class HDPutAllPartitionAwareOperationFactory extends PutAllPartitionAwareOperationFactory {

    @SuppressWarnings("unused")
    public HDPutAllPartitionAwareOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public HDPutAllPartitionAwareOperationFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        super(name, partitions, mapEntries);
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new HDPutAllOperation(name, mapEntries[i]);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }
}
