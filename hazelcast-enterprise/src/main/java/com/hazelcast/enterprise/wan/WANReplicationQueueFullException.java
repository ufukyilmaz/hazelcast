package com.hazelcast.enterprise.wan;


import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.core.HazelcastException;

/**
 * A {@link com.hazelcast.core.HazelcastException} that is thrown when the wan replication queues are full
 *
 * This exception is only thrown when WAN is configured with
 * {@link WANQueueFullBehavior#THROW_EXCEPTION}
 */
public class WANReplicationQueueFullException extends HazelcastException {
}
