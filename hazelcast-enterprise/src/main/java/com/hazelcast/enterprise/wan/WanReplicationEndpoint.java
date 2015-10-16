/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

/**
 * Implementations of this interface represent a replication endpoint, normally another
 * Hazelcast cluster only reachable over a wide area network.
 */
public interface WanReplicationEndpoint
        extends WanReplicationPublisher {

    /**
     * Initializes the endpoint using the given arguments.
     *
     * @param node      the current node that tries to connect
     * @param wanReplicationName the name of {@link com.hazelcast.config.WanReplicationConfig} config
     * @param targetClusterConfig  this endpoint will be initialized using this {@link WanTargetClusterConfig} instance
     */
    void init(Node node, String wanReplicationName, WanTargetClusterConfig targetClusterConfig, boolean snapshotEnabled);

    /**
     * Closes the endpoint and its internal connections and shuts down other internal states
     */
    void shutdown();

    void removeBackup(WanReplicationEvent wanReplicationEvent);

    void putBackup(WanReplicationEvent wanReplicationEvent);

    PublisherQueueContainer getPublisherQueueContainer();

    void addMapQueue(String key, int partitionId, WanReplicationEventQueue value);

    void addCacheQueue(String key, int partitionId, WanReplicationEventQueue value);

    void pause();

    void resume();
}
