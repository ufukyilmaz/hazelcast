package com.hazelcast.enterprise.wan;

import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Wan replication event queue wrapper
 */
public class WanReplicationEventQueue extends ConcurrentLinkedQueue<WanReplicationEvent> {
}
