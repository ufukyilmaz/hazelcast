package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.wan.WanReplicationPublisher;

/**
 * Interface to be implemented by custom WAN event consumers. Wan replication
 * consumers are typically used in conjunction with a custom
 * {@link WanReplicationPublisher}. The publisher will then publish events
 * in a custom fashion which the consumer expects and processes accordingly.
 * This way, you can provide custom publication and consumption mechanisms
 * and protocols for WAN replication. The default implementation of the WAN
 * publisher ignores any configured custom consumers and the WAN events will
 * be processed as if there was no custom consumer implementation.
 * Can be registered by programmatically or by XML using {@link WanConsumerConfig}.
 *
 * @see WanReplicationPublisher
 */
public interface WanReplicationConsumer {

    /**
     * Initialize the WAN consumer. The method is invoked once the node has
     * started.
     * The WAN consumer is responsible for registering itself for WAN event
     * consumption. Typically this means that you would either use the
     * {@link com.hazelcast.spi.impl.executionservice.ExecutionService}
     * to schedule the consumer to run periodically or continually by having an
     * implementation which uses blocking or spinning locks to check for new
     * events. The implementation is free however to choose another approach.
     *
     * @param node               this node
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param config             the WAN consumer config
     */
    void init(Node node, String wanReplicationName, WanConsumerConfig config);

    /**
     * Callback method to shutdown the WAN replication consumer. This is called
     * on node shutdown.
     */
    void shutdown();
}