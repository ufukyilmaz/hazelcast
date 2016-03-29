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

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.LocalWanPublisherStats;
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
     * @param wanReplicationConfig  {@link com.hazelcast.config.WanReplicationConfig} config
     * @param wanPublisherConfig  this endpoint will be initialized using this {@link WanPublisherConfig} instance
     */
    void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig);

    /**
     * Closes the endpoint and its internal connections and shuts down other internal states
     */
    void shutdown();

    void removeBackup(WanReplicationEvent wanReplicationEvent);

    void putBackup(WanReplicationEvent wanReplicationEvent);

    PublisherQueueContainer getPublisherQueueContainer();

    void addMapQueue(String key, int partitionId, WanReplicationEventQueue value);

    void addCacheQueue(String key, int partitionId, WanReplicationEventQueue value);

    /**
     * Calls to this method will pause wan event queue polling. Effectively, pauses wan replication for
     * its {@link WanReplicationEndpoint} instance.
     *
     * Wan events will still be offered to wan replication
     * queues but they won't be polled.
     *
     * Calling this method on already paused {@link WanReplicationEndpoint} instances will have no effect.
     */
    void pause();

    /**
     *  This method re-enables wan event queue polling for a paused {@link WanReplicationEndpoint} instance.
     *
     *  Calling this method on already running {@link WanReplicationEndpoint} instances will have no effect.
     *
     *  @see #pause()
     */
    void resume();

    /**
     * Gathers statistics of related {@link WanReplicationEndpoint} instance.
     *
     * @return {@link LocalWanPublisherStats}
     */
    LocalWanPublisherStats getStats();

    void checkWanReplicationQueues();
}
