/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise implementation for WAN replication
 */
public class EnterpriseWanReplicationService
        implements WanReplicationService {

    private final Object lock = new Object();

    private final Node node;
    private final ILogger logger;

    private final Map<String, WanReplicationPublisherDelegate> wanReplications = initializeWebReplicationPublisherMapping();

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        WanReplicationPublisherDelegate wr = wanReplications.get(name);
        if (wr != null) {
            return wr;
        }
        synchronized (lock) {
            wr = wanReplications.get(name);
            if (wr != null) {
                return wr;
            }
            WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) {
                return null;
            }
            List<WanTargetClusterConfig> targets = wanReplicationConfig.getTargetClusterConfigs();
            WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[targets.size()];
            int count = 0;
            for (WanTargetClusterConfig targetClusterConfig : targets) {
                WanReplicationEndpoint target;
                if (targetClusterConfig.getReplicationImpl() != null) {
                    try {
                        target = ClassLoaderUtil
                                .newInstance(node.getConfigClassLoader(), targetClusterConfig.getReplicationImpl());
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                } else {
                    target = new WanNoDelayReplication();
                }
                String groupName = targetClusterConfig.getGroupName();
                String password = targetClusterConfig.getGroupPassword();
                String[] addresses = new String[targetClusterConfig.getEndpoints().size()];
                targetClusterConfig.getEndpoints().toArray(addresses);
                target.init(node, groupName, password, addresses);
                targetEndpoints[count++] = target;
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    @Override
    public void handleEvent(final Packet packet) {
        // todo execute in which thread
        node.nodeEngine.getExecutionService().execute("hz:wan", new Runnable() {
            @Override
            public void run() {
                final Data data = packet.getData();
                try {
                    WanReplicationEvent replicationEvent = (WanReplicationEvent) node.nodeEngine.toObject(data);
                    String serviceName = replicationEvent.getServiceName();
                    ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
                    service.onReplicationEvent(replicationEvent);
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
        });
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
                WanReplicationEndpoint[] wanReplicationEndpoints = wanReplication.getEndpoints();
                if (wanReplicationEndpoints != null) {
                    for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints) {
                        if (wanReplicationEndpoint != null) {
                            wanReplicationEndpoint.shutdown();
                        }
                    }
                }
            }
            wanReplications.clear();
        }
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWebReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }
}
