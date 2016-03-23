package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.instance.Node;

/**
 * Interface to be implemented by custom WAN event consumers.
 *
 * Can be registered by programmatically or by xml using {@link WanConsumerConfig}
 */
public interface WanReplicationConsumer {

    void init(Node node, String wanReplicationName, WanConsumerConfig config);
    void shutdown();

}
